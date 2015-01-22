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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.mbean.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.product.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.authentication.*;
import org.apache.ignite.spi.authentication.noop.*;
import org.apache.ignite.hadoop.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.checkpoint.*;
import org.apache.ignite.internal.managers.collision.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.managers.failover.*;
import org.apache.ignite.internal.managers.indexing.*;
import org.apache.ignite.internal.managers.loadbalancer.*;
import org.apache.ignite.internal.managers.securesession.*;
import org.apache.ignite.internal.managers.security.*;
import org.apache.ignite.internal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.clock.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.kernal.processors.dataload.*;
import org.apache.ignite.internal.processors.email.*;
import org.gridgain.grid.kernal.processors.interop.*;
import org.apache.ignite.internal.processors.job.*;
import org.apache.ignite.internal.processors.jobmetrics.*;
import org.apache.ignite.internal.processors.license.*;
import org.apache.ignite.internal.processors.offheap.*;
import org.apache.ignite.internal.processors.plugin.*;
import org.apache.ignite.internal.processors.port.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.resource.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.apache.ignite.internal.processors.segmentation.*;
import org.apache.ignite.internal.processors.service.*;
import org.apache.ignite.internal.processors.session.*;
import org.apache.ignite.internal.processors.streamer.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.spi.securesession.noop.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.nodestart.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.io.*;
import java.lang.management.*;
import java.lang.reflect.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.GridKernalState.*;
import static org.apache.ignite.lifecycle.LifecycleEventType.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.GridComponentType.*;
import static org.apache.ignite.internal.GridNodeAttributes.*;
import static org.apache.ignite.internal.GridProductImpl.*;
import static org.apache.ignite.internal.processors.license.GridLicenseSubsystem.*;
import static org.apache.ignite.internal.util.nodestart.GridNodeStartUtils.*;

/**
 * GridGain kernal.
 * <p/>
 * See <a href="http://en.wikipedia.org/wiki/Kernal">http://en.wikipedia.org/wiki/Kernal</a> for information on the
 * misspelling.
 */
public class GridKernal extends ClusterGroupAdapter implements GridEx, IgniteMBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** Compatible versions. */
    private static final String COMPATIBLE_VERS = GridProperties.get("gridgain.compatible.vers");

    /** GridGain site that is shown in log messages. */
    static final String SITE = "www.gridgain.com";

    /** System line separator. */
    private static final String NL = U.nl();

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_DELAY = 1000 * 60 * 60; // Every hour.

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_CONN_TIMEOUT = 10 * 1000; // 10 seconds.

    /** Periodic version check delay. */
    private static final long PERIODIC_LIC_CHECK_DELAY = 1000 * 60; // Every minute.

    /** Periodic starvation check interval. */
    private static final long PERIODIC_STARVATION_CHECK_FREQ = 1000 * 30;

    /** Shutdown delay in msec. when license violation detected. */
    private static final int SHUTDOWN_DELAY = 60 * 1000;

    /** */
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

    /** Kernal start timestamp. */
    private long startTime = U.currentTimeMillis();

    /** Spring context, potentially {@code null}. */
    private GridSpringResourceContext rsrcCtx;

    /** */
    @GridToStringExclude
    private Timer updateNtfTimer;

    /** */
    @GridToStringExclude
    private Timer starveTimer;

    /** */
    @GridToStringExclude
    private Timer licTimer;

    /** */
    @GridToStringExclude
    private Timer metricsLogTimer;

    /** Indicate error on grid stop. */
    @GridToStringExclude
    private boolean errOnStop;

    /** Node local store. */
    @GridToStringExclude
    private ClusterNodeLocalMap nodeLoc;

    /** Scheduler. */
    @GridToStringExclude
    private IgniteScheduler scheduler;

    /** Grid security instance. */
    @GridToStringExclude
    private GridSecurity security;

    /** Portables instance. */
    @GridToStringExclude
    private IgnitePortables portables;

    /** Kernal gateway. */
    @GridToStringExclude
    private final AtomicReference<GridKernalGateway> gw = new AtomicReference<>();

    /** Data Grid edition usage registered flag. */
    @GridToStringExclude
    private volatile boolean dbUsageRegistered;

    /** */
    @GridToStringExclude
    private final Collection<String> compatibleVers;

    /** Stop guard. */
    @GridToStringExclude
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * No-arg constructor is required by externalization.
     */
    public GridKernal() {
        this(null);
    }

    /**
     * @param rsrcCtx Optional Spring application context.
     */
    public GridKernal(@Nullable GridSpringResourceContext rsrcCtx) {
        super(null, null, null, (IgnitePredicate<ClusterNode>)null);

        this.rsrcCtx = rsrcCtx;

        String[] compatibleVers = COMPATIBLE_VERS.split(",");

        for (int i = 0; i < compatibleVers.length; i++)
            compatibleVers[i] = compatibleVers[i].trim();

        this.compatibleVers = Collections.unmodifiableList(Arrays.asList(compatibleVers));
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public final IgniteCompute compute(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).compute();
    }

    /** {@inheritDoc} */
    @Override public final IgniteMessaging message(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).message();
    }

    /** {@inheritDoc} */
    @Override public final IgniteEvents events(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).events();
    }

    /** {@inheritDoc} */
    @Override public IgniteManaged managed(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).managed();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).executorService();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return gridName;
    }

    /** {@inheritDoc} */
    @Override public String getCopyright() {
        return ctx.product().copyright();
    }

    /** {@inheritDoc} */
    @Override public String getLicenseFilePath() {
        assert cfg != null;

        return cfg.getLicenseUrl();
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
        return COMPOUND_VER + '-' + BUILD_TSTAMP_STR;
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
    @Override public String getAuthenticationSpiFormatted() {
        assert cfg != null;

        return cfg.getAuthenticationSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getSecureSessionSpiFormatted() {
        assert cfg != null;

        return cfg.getSecureSessionSpi().toString();
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

        return cfg.getExecutorService().toString();
    }

    /** {@inheritDoc} */
    @Override public String getGridGainHome() {
        assert cfg != null;

        return cfg.getGridGainHome();
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
    @Override public Collection<String> getUserAttributesFormatted() {
        assert cfg != null;

        return F.transform(cfg.getUserAttributes().entrySet(), new C1<Map.Entry<String, ?>, String>() {
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
    @Override public Collection<String> getLifecycleBeansFormatted() {
        LifecycleBean[] beans = cfg.getLifecycleBeans();

        return F.isEmpty(beans) ? Collections.<String>emptyList() : F.transform(beans, F.<LifecycleBean>string());
    }

    /**
     * @param attrs Current attributes.
     * @param name  New attribute name.
     * @param val New attribute value.
     * @throws IgniteCheckedException If duplicated SPI name found.
     */
    private void add(Map<String, Object> attrs, String name, @Nullable Serializable val) throws IgniteCheckedException {
        assert attrs != null;
        assert name != null;

        if (attrs.put(name, val) != null) {
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
        if (!cfg.isDaemon() && cfg.getLifecycleBeans() != null)
            for (LifecycleBean bean : cfg.getLifecycleBeans())
                if (bean != null)
                    bean.onLifecycleEvent(evt);
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
                ", gridName=" + gridName + ']', e);
        }
    }

    /**
     * @param cfg Grid configuration to use.
     * @param utilityCachePool Utility cache pool.
     * @param errHnd Error handler to use for notification about startup problems.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings({"CatchGenericClass", "unchecked"})
    public void start(final IgniteConfiguration cfg, ExecutorService utilityCachePool, GridAbsClosure errHnd)
        throws IgniteCheckedException {
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

        log = (GridLoggerProxy)cfg.getGridLogger().getLogger(getClass().getName() +
            (gridName != null ? '%' + gridName : ""));

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
        ackSmtpConfiguration();
        ackCacheConfiguration();
        ackP2pConfiguration();

        // Run background network diagnostics.
        GridDiagnostic.runBackgroundCheck(gridName, cfg.getExecutorService(), log);

        boolean notifyEnabled = IgniteSystemProperties.getBoolean(GG_UPDATE_NOTIFIER, true);

        GridUpdateNotifier verChecker0 = null;

        if (notifyEnabled) {
            try {
                verChecker0 = new GridUpdateNotifier(gridName, VER, SITE, gw, false);

                verChecker0.checkForNewVersion(cfg.getExecutorService(), log);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create GridUpdateNotifier: " + e);
            }
        }

        final GridUpdateNotifier verChecker = verChecker0;

        // Ack 3-rd party licenses location.
        if (log.isInfoEnabled() && cfg.getGridGainHome() != null)
            log.info("3-rd party licenses can be found at: " + cfg.getGridGainHome() + File.separatorChar + "libs" +
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

        Map<String, Object> attrs = createNodeAttributes(cfg, BUILD_TSTAMP_STR);

        // Spin out SPIs & managers.
        try {
            GridKernalContextImpl ctx =
                new GridKernalContextImpl(log, this, cfg, gw, utilityCachePool, ENT);

            nodeLoc = new ClusterNodeLocalMapImpl(ctx);

            U.onGridStart();

            // Set context into rich adapter.
            setKernalContext(ctx);

            // Start and configure resource processor first as it contains resources used
            // by all other managers and processors.
            GridResourceProcessor rsrcProc = new GridResourceProcessor(ctx);

            rsrcProc.setSpringContext(rsrcCtx);

            ctx.product(new GridProductImpl(ctx, verChecker));

            scheduler = new IgniteSchedulerImpl(ctx);

            startProcessor(ctx, rsrcProc, attrs);

            // Inject resources into lifecycle beans.
            if (!cfg.isDaemon() && cfg.getLifecycleBeans() != null)
                for (LifecycleBean bean : cfg.getLifecycleBeans())
                    if (bean != null)
                        rsrcProc.inject(bean);

            // Lifecycle notification.
            notifyLifecycleBeans(BEFORE_GRID_START);

            // Starts lifecycle aware components.
            U.startLifecycleAware(lifecycleAwares(cfg));

            addHelper(ctx, GGFS_HELPER.create(F.isEmpty(cfg.getGgfsConfiguration())));

            startProcessor(ctx, new IgnitePluginProcessor(ctx, cfg), attrs);

            // Off-heap processor has no dependencies.
            startProcessor(ctx, new GridOffHeapProcessor(ctx), attrs);

            // Closure processor should be started before all others
            // (except for resource processor), as many components can depend on it.
            startProcessor(ctx, new GridClosureProcessor(ctx), attrs);

            // Start some other processors (order & place is important).
            startProcessor(ctx, (GridProcessor)EMAIL.create(ctx, cfg.getSmtpHost() == null), attrs);
            startProcessor(ctx, new GridPortProcessor(ctx), attrs);
            startProcessor(ctx, new GridJobMetricsProcessor(ctx), attrs);

            // Timeout processor needs to be started before managers,
            // as managers may depend on it.
            startProcessor(ctx, new GridTimeoutProcessor(ctx), attrs);

            // Start SPI managers.
            // NOTE: that order matters as there are dependencies between managers.
            startManager(ctx, createComponent(GridSecurityManager.class, ctx), attrs);
            startManager(ctx, createComponent(GridSecureSessionManager.class, ctx), attrs);
            startManager(ctx, new GridIoManager(ctx), attrs);
            startManager(ctx, new GridCheckpointManager(ctx), attrs);

            startManager(ctx, new GridEventStorageManager(ctx), attrs);
            startManager(ctx, new GridDeploymentManager(ctx), attrs);
            startManager(ctx, new GridLoadBalancerManager(ctx), attrs);
            startManager(ctx, new GridFailoverManager(ctx), attrs);
            startManager(ctx, new GridCollisionManager(ctx), attrs);
            startManager(ctx, new GridSwapSpaceManager(ctx), attrs);
            startManager(ctx, new GridIndexingManager(ctx), attrs);

            ackSecurity(ctx);

            // Start processors before discovery manager, so they will
            // be able to start receiving messages once discovery completes.
            startProcessor(ctx, new GridClockSyncProcessor(ctx), attrs);
            startProcessor(ctx, createComponent(GridLicenseProcessor.class, ctx), attrs);
            startProcessor(ctx, new GridAffinityProcessor(ctx), attrs);
            startProcessor(ctx, createComponent(GridSegmentationProcessor.class, ctx), attrs);
            startProcessor(ctx, new GridQueryProcessor(ctx), attrs);
            startProcessor(ctx, new GridCacheProcessor(ctx), attrs);
            startProcessor(ctx, new GridTaskSessionProcessor(ctx), attrs);
            startProcessor(ctx, new GridJobProcessor(ctx), attrs);
            startProcessor(ctx, new GridTaskProcessor(ctx), attrs);
            startProcessor(ctx, (GridProcessor)SCHEDULE.createOptional(ctx), attrs);
            startProcessor(ctx, createComponent(GridPortableProcessor.class, ctx), attrs);
            startProcessor(ctx, createComponent(GridInteropProcessor.class, ctx), attrs);
            startProcessor(ctx, new GridRestProcessor(ctx), attrs);
            startProcessor(ctx, new GridDataLoaderProcessor(ctx), attrs);
            startProcessor(ctx, new GridStreamProcessor(ctx), attrs);
            startProcessor(ctx, (GridProcessor)GGFS.create(ctx, F.isEmpty(cfg.getGgfsConfiguration())), attrs);
            startProcessor(ctx, new GridContinuousProcessor(ctx), attrs);
            startProcessor(ctx, (GridProcessor)(cfg.isPeerClassLoadingEnabled() ?
                GridComponentType.HADOOP.create(ctx, true): // No-op when peer class loading is enabled.
                GridComponentType.HADOOP.createIfInClassPath(ctx, cfg.getHadoopConfiguration() != null)), attrs);
            startProcessor(ctx, new GridServiceProcessor(ctx), attrs);

            // Start plugins.
            for (PluginProvider provider : ctx.plugins().allProviders()) {
                ctx.add(new GridPluginComponent(provider));

                provider.start(ctx.plugins().pluginContextForProvider(provider), attrs);
            }

            ctx.createMessageFactory();

            if (ctx.isEnterprise()) {
                security = new GridSecurityImpl(ctx);
                portables = new GridPortablesImpl(ctx);
            }

            gw.writeLock();

            try {
                gw.setState(STARTED);

                // Start discovery manager last to make sure that grid is fully initialized.
                startManager(ctx, new GridDiscoveryManager(ctx), attrs);
            }
            finally {
                gw.writeUnlock();
            }

            // Check whether physical RAM is not exceeded.
            checkPhysicalRam();

            // Suggest configuration optimizations.
            suggestOptimizations(ctx, cfg);

            if (!ctx.isEnterprise())
                warnNotSupportedFeaturesForOs(cfg);

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

                comp.onKernalStart();
            }

            // Ack the license.
            ctx.license().ackLicense();

            // Register MBeans.
            registerKernalMBean();
            registerLocalNodeMBean();
            registerExecutorMBeans();

            // Lifecycle bean notifications.
            notifyLifecycleBeans(AFTER_GRID_START);
        }
        catch (Throwable e) {
            IgniteSpiVersionCheckException verCheckErr = X.cause(e, IgniteSpiVersionCheckException.class);

            if (verCheckErr != null)
                U.error(log, verCheckErr.getMessage());
            else if (X.hasCause(e, InterruptedException.class, IgniteInterruptedException.class))
                U.warn(log, "Grid startup routine has been interrupted (will rollback).");
            else
                U.error(log, "Got exception while starting (will rollback startup routine).", e);

            errHnd.apply();

            stop(true);

            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;
            else
                throw new IgniteCheckedException(e);
        }

        // Mark start timestamp.
        startTime = U.currentTimeMillis();

        // Ack latest version information.
        if (verChecker != null)
            verChecker.reportStatus(log);

        if (notifyEnabled) {
            assert verChecker != null;

            verChecker.reportOnlyNew(true);
            verChecker.licenseProcessor(ctx.license());

            updateNtfTimer = new Timer("gridgain-update-notifier-timer");

            // Setup periodic version check.
            updateNtfTimer.scheduleAtFixedRate(new GridTimerTask() {
                @Override public void safeRun() throws InterruptedException {
                    verChecker.topologySize(nodes().size());

                    verChecker.checkForNewVersion(cfg.getExecutorService(), log);

                    // Just wait for 10 secs.
                    Thread.sleep(PERIODIC_VER_CHECK_CONN_TIMEOUT);

                    // Report status if one is available.
                    // No-op if status is NOT available.
                    verChecker.reportStatus(log);
                }
            }, PERIODIC_VER_CHECK_DELAY, PERIODIC_VER_CHECK_DELAY);
        }

        String intervalStr = IgniteSystemProperties.getString(GG_STARVATION_CHECK_INTERVAL);

        // Start starvation checker if enabled.
        boolean starveCheck = !isDaemon() && !"0".equals(intervalStr);

        if (starveCheck) {
            final long interval = F.isEmpty(intervalStr) ? PERIODIC_STARVATION_CHECK_FREQ : Long.parseLong(intervalStr);

            starveTimer = new Timer("gridgain-starvation-checker");

            starveTimer.scheduleAtFixedRate(new GridTimerTask() {
                /** Last completed task count. */
                private long lastCompletedCnt;

                @Override protected void safeRun() {
                    ExecutorService e = cfg.getExecutorService();

                    if (!(e instanceof ThreadPoolExecutor))
                        return;

                    ThreadPoolExecutor exec = (ThreadPoolExecutor)e;

                    long completedCnt = exec.getCompletedTaskCount();

                    // If all threads are active and no task has completed since last time and there is
                    // at least one waiting request, then it is possible starvation.
                    if (exec.getPoolSize() == exec.getActiveCount() && completedCnt == lastCompletedCnt &&
                        !exec.getQueue().isEmpty())
                        LT.warn(log, null, "Possible thread pool starvation detected (no task completed in last " +
                            interval + "ms, is executorService pool size large enough?)");

                    lastCompletedCnt = completedCnt;
                }
            }, interval, interval);
        }

        if (!isDaemon()) {
            licTimer = new Timer("gridgain-license-checker");

            // Setup periodic license check.
            licTimer.scheduleAtFixedRate(new GridTimerTask() {
                @Override public void safeRun() throws InterruptedException {
                    try {
                        ctx.license().checkLicense();
                    }
                    // This exception only happens when license processor was unable
                    // to resolve license violation on its own and this grid instance
                    // now needs to be shutdown.
                    //
                    // Note that in most production configurations the license will
                    // have certain grace period and license processor will attempt
                    // to reload the license during the grace period.
                    //
                    // This exception thrown here means that grace period, if any,
                    // has expired and license violation is still unresolved.
                    catch (IgniteProductLicenseException ignored) {
                        U.error(log, "License violation is unresolved. GridGain node will shutdown in " +
                            (SHUTDOWN_DELAY / 1000) + " sec.");
                        U.error(log, "  ^-- Contact your support for immediate assistance (!)");

                        // Allow interruption to break from here since
                        // node is stopping anyways.
                        Thread.sleep(SHUTDOWN_DELAY);

                        G.stop(gridName, true);
                    }
                    // Safety net.
                    catch (Throwable e) {
                        U.error(log, "Unable to check the license due to system error.", e);
                        U.error(log, "Grid instance will be stopped...");

                        // Stop the grid if we get unknown license-related error.
                        // Should never happen. Practically an assertion...
                        G.stop(gridName, true);
                    }
                }
            }, PERIODIC_LIC_CHECK_DELAY, PERIODIC_LIC_CHECK_DELAY);
        }

        long metricsLogFreq = cfg.getMetricsLogFrequency();

        if (metricsLogFreq > 0) {
            metricsLogTimer = new Timer("gridgain-metrics-logger");

            metricsLogTimer.scheduleAtFixedRate(new GridTimerTask() {
                /** */
                private final DecimalFormat dblFmt = new DecimalFormat("#.##");

                @Override protected void safeRun() {
                    if (log.isInfoEnabled()) {
                        ClusterNodeMetrics m = localNode().metrics();

                        double cpuLoadPct = m.getCurrentCpuLoad() * 100;
                        double avgCpuLoadPct = m.getAverageCpuLoad() * 100;
                        double gcPct = m.getCurrentGcCpuLoad() * 100;

                        long heapUsed = m.getHeapMemoryUsed();
                        long heapMax = m.getHeapMemoryMaximum();

                        long heapUsedInMBytes = heapUsed / 1024 / 1024;
                        long heapCommInMBytes = m.getHeapMemoryCommitted() / 1024 / 1024;

                        double freeHeapPct = heapMax > 0 ? ((double)((heapMax - heapUsed) * 100)) / heapMax : -1;

                        int hosts = 0;
                        int nodes = 0;
                        int cpus = 0;

                        try {
                            ClusterMetrics metrics = metrics();

                            hosts = metrics.getTotalHosts();
                            nodes = metrics.getTotalNodes();
                            cpus = metrics.getTotalCpus();
                        }
                        catch (IgniteCheckedException ignore) {
                        }

                        int pubPoolActiveThreads = 0;
                        int pubPoolIdleThreads = 0;
                        int pubPoolQSize = 0;

                        ExecutorService pubExec = cfg.getExecutorService();

                        if (pubExec instanceof ThreadPoolExecutor) {
                            ThreadPoolExecutor exec = (ThreadPoolExecutor)pubExec;

                            int poolSize = exec.getPoolSize();

                            pubPoolActiveThreads = Math.min(poolSize, exec.getActiveCount());
                            pubPoolIdleThreads = poolSize - pubPoolActiveThreads;
                            pubPoolQSize = exec.getQueue().size();
                        }

                        int sysPoolActiveThreads = 0;
                        int sysPoolIdleThreads = 0;
                        int sysPoolQSize = 0;

                        ExecutorService sysExec = cfg.getSystemExecutorService();

                        if (sysExec instanceof ThreadPoolExecutor) {
                            ThreadPoolExecutor exec = (ThreadPoolExecutor)sysExec;

                            int poolSize = exec.getPoolSize();

                            sysPoolActiveThreads = Math.min(poolSize, exec.getActiveCount());
                            sysPoolIdleThreads = poolSize - sysPoolActiveThreads;
                            sysPoolQSize = exec.getQueue().size();
                        }

                        String msg = NL +
                            "Metrics for local node (to disable set 'metricsLogFrequency' to 0)" + NL +
                            "    ^-- H/N/C [hosts=" + hosts + ", nodes=" + nodes + ", CPUs=" + cpus + "]" + NL +
                            "    ^-- CPU [cur=" + dblFmt.format(cpuLoadPct) + "%, avg=" +
                                dblFmt.format(avgCpuLoadPct) + "%, GC=" + dblFmt.format(gcPct) + "%]" + NL +
                            "    ^-- Heap [used=" + dblFmt.format(heapUsedInMBytes) + "MB, free=" +
                                dblFmt.format(freeHeapPct) + "%, comm=" + dblFmt.format(heapCommInMBytes) + "MB]" + NL +
                            "    ^-- Public thread pool [active=" + pubPoolActiveThreads + ", idle=" +
                                pubPoolIdleThreads + ", qSize=" + pubPoolQSize + "]" + NL +
                            "    ^-- System thread pool [active=" + sysPoolActiveThreads + ", idle=" +
                                sysPoolIdleThreads + ", qSize=" + sysPoolQSize + "]" + NL +
                            "    ^-- Outbound messages queue [size=" + m.getOutboundMessagesQueueSize() + "]";

                        log.info(msg);
                    }
                }
            }, metricsLogFreq, metricsLogFreq);
        }

        ctx.performance().logSuggestions(log, gridName);

        ackBenchmarks();
        ackVisor();

        ackStart(rtBean);

        if (!isDaemon())
            ctx.discovery().ackTopology();

        // Send node start email notification, if enabled.
        if (isSmtpEnabled() && isAdminEmailsSet() && cfg.isLifeCycleEmailNotification()) {
            SB sb = new SB();

            for (GridPortRecord rec : ctx.ports().records())
                sb.a(rec.protocol()).a(":").a(rec.port()).a(" ");

            String nid = localNode().id().toString().toUpperCase();
            String nid8 = U.id8(localNode().id()).toUpperCase();

            IgniteProductLicense lic = ctx.license().license();

            String body =
                "GridGain node started with the following parameters:" + NL +
                NL +
                "----" + NL +
                "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH + NL +
                "Grid name: " + gridName + NL +
                "Node ID: " + nid + NL +
                "Node order: " + localNode().order() + NL +
                "Node addresses: " + U.addressesAsString(localNode()) + NL +
                "Local ports: " + sb + NL +
                "OS name: " + U.osString() + NL +
                "OS user: " + System.getProperty("user.name") + NL +
                "CPU(s): " + localNode().metrics().getTotalCpus() + NL +
                "Heap: " + U.heapSize(localNode(), 2) + "GB" + NL +
                "JVM name: " + U.jvmName() + NL +
                "JVM vendor: " + U.jvmVendor() + NL +
                "JVM version: " + U.jvmVersion() + NL +
                "VM name: " + rtBean.getName() + NL;

            if (lic != null) {
                body +=
                    "License ID: " + lic.id().toString().toUpperCase() + NL +
                    "Licensed to: " + lic.userOrganization() + NL;
            }
            else
                assert !ENT;

            body +=
                "----" + NL +
                NL +
                "NOTE:" + NL +
                "This message is sent automatically to all configured admin emails." + NL +
                "To change this behavior use 'lifeCycleEmailNotify' grid configuration property." +
                NL + NL +
                "| " + SITE + NL +
                "| support@gridgain.com" + NL;

            sendAdminEmailAsync("GridGain node started: " + nid8, body, false);
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
        A.notNull(cfg.getExecutorService(), "cfg.getExecutorService()");
        A.notNull(cfg.getUserAttributes(), "cfg.getUserAttributes()");

        // All SPIs should be non-null.
        A.notNull(cfg.getSwapSpaceSpi(), "cfg.getSwapSpaceSpi()");
        A.notNull(cfg.getCheckpointSpi(), "cfg.getCheckpointSpi()");
        A.notNull(cfg.getCommunicationSpi(), "cfg.getCommunicationSpi()");
        A.notNull(cfg.getDeploymentSpi(), "cfg.getDeploymentSpi()");
        A.notNull(cfg.getDiscoverySpi(), "cfg.getDiscoverySpi()");
        A.notNull(cfg.getEventStorageSpi(), "cfg.getEventStorageSpi()");
        A.notNull(cfg.getAuthenticationSpi(), "cfg.getAuthenticationSpi()");
        A.notNull(cfg.getSecureSessionSpi(), "cfg.getSecureSessionSpi()");
        A.notNull(cfg.getCollisionSpi(), "cfg.getCollisionSpi()");
        A.notNull(cfg.getFailoverSpi(), "cfg.getFailoverSpi()");
        A.notNull(cfg.getLoadBalancingSpi(), "cfg.getLoadBalancingSpi()");
        A.notNull(cfg.getIndexingSpi(), "cfg.getIndexingSpi()");

        A.ensure(cfg.getNetworkTimeout() > 0, "cfg.getNetworkTimeout() > 0");
        A.ensure(cfg.getNetworkSendRetryDelay() > 0, "cfg.getNetworkSendRetryDelay() > 0");
        A.ensure(cfg.getNetworkSendRetryCount() > 0, "cfg.getNetworkSendRetryCount() > 0");

        if (!F.isEmpty(cfg.getPluginConfigurations())) {
            for (PluginConfiguration pluginCfg : cfg.getPluginConfigurations())
                A.notNull(pluginCfg.providerClass(), "PluginConfiguration.providerClass()");
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
     * @param ctx Context.
     * @param cfg Configuration to check for possible performance issues.
     */
    private void suggestOptimizations(GridKernalContext ctx, IgniteConfiguration cfg) {
        GridPerformanceSuggestions perf = ctx.performance();

        if (ctx.collision().enabled())
            perf.add("Disable collision resolution (remove 'collisionSpi' from configuration)");

        if (ctx.checkpoint().enabled())
            perf.add("Disable checkpoints (remove 'checkpointSpi' from configuration)");

        if (cfg.isPeerClassLoadingEnabled())
            perf.add("Disable peer class loading (set 'peerClassLoadingEnabled' to false)");

        if (cfg.isMarshalLocalJobs())
            perf.add("Disable local jobs marshalling (set 'marshalLocalJobs' to false)");

        if (cfg.getIncludeEventTypes() != null && cfg.getIncludeEventTypes().length != 0)
            perf.add("Disable grid events (remove 'includeEventTypes' from configuration)");

        if (IgniteOptimizedMarshaller.available() && !(cfg.getMarshaller() instanceof IgniteOptimizedMarshaller))
            perf.add("Enable optimized marshaller (set 'marshaller' to " +
                IgniteOptimizedMarshaller.class.getSimpleName() + ')');
    }

    /**
     * Warns user about unsupported features which was configured in OS edition.
     *
     * @param cfg Grid configuration.
     */
    private void warnNotSupportedFeaturesForOs(IgniteConfiguration cfg) {
        Collection<String> msgs = new ArrayList<>();

        if (!F.isEmpty(cfg.getSegmentationResolvers()))
            msgs.add("Network segmentation detection.");

        if (cfg.getSecureSessionSpi() != null && !(cfg.getSecureSessionSpi() instanceof NoopSecureSessionSpi))
            msgs.add("Secure session SPI.");

        if (cfg.getAuthenticationSpi() != null && !(cfg.getAuthenticationSpi() instanceof NoopAuthenticationSpi))
            msgs.add("Authentication SPI.");

        if (!F.isEmpty(msgs)) {
            U.quietAndInfo(log, "The following features are not supported in open source edition, " +
                "related configuration settings will be ignored " +
                "(consider downloading enterprise edition from http://www.gridgain.com):");

            for (String s : msgs)
                U.quietAndInfo(log, "  ^-- " + s);

            U.quietAndInfo(log, "");
        }
    }

    /**
     * Creates attributes map and fills it in.
     *
     * @param cfg Grid configuration.
     * @param build Build string.
     * @return Map of all node attributes.
     * @throws IgniteCheckedException thrown if was unable to set up attribute.
     */
    @SuppressWarnings({"SuspiciousMethodCalls", "unchecked", "TypeMayBeWeakened"})
    private Map<String, Object> createNodeAttributes(IgniteConfiguration cfg, String build) throws IgniteCheckedException {
        Map<String, Object> attrs = new HashMap<>();

        final String[] incProps = cfg.getIncludeProperties();

        try {
            // Stick all environment settings into node attributes.
            attrs.putAll(F.view(System.getenv(), new P1<String>() {
                @Override public boolean apply(String name) {
                    return incProps == null || U.containsStringArray(incProps, name, true) ||
                        U.isVisorNodeStartProperty(name) || U.isVisorRequiredProperty(name);
                }
            }));

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
            for (Map.Entry<Object, Object> e : F.view(System.getProperties(), new P1<Object>() {
                @Override public boolean apply(Object o) {
                    String name = (String)o;

                    return incProps == null || U.containsStringArray(incProps, name, true) ||
                        U.isVisorRequiredProperty(name);
                }
            }).entrySet()) {
                Object val = attrs.get(e.getKey());

                if (val != null && !val.equals(e.getValue()))
                    U.warn(log, "System property will override environment variable with the same name: "
                        + e.getKey());

                attrs.put((String)e.getKey(), e.getValue());
            }

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
            U.warn(log, "GridGain is starting on loopback address... Only nodes on the same physical " +
                "computer can participate in topology.",
                "GridGain is starting on loopback address...");

        // Stick in network context into attributes.
        add(attrs, ATTR_IPS, (ips.isEmpty() ? "" : ips));
        add(attrs, ATTR_MACS, (macs.isEmpty() ? "" : macs));

        // Stick in some system level attributes
        add(attrs, ATTR_JIT_NAME, U.getCompilerMx() == null ? "" : U.getCompilerMx().getName());
        add(attrs, ATTR_BUILD_VER, COMPOUND_VER);
        add(attrs, ATTR_BUILD_DATE, build);
        add(attrs, ATTR_COMPATIBLE_VERS, (Serializable)compatibleVersions());
        add(attrs, ATTR_MARSHALLER, cfg.getMarshaller().getClass().getName());
        add(attrs, ATTR_USER_NAME, System.getProperty("user.name"));
        add(attrs, ATTR_GRID_NAME, gridName);

        add(attrs, ATTR_PEER_CLASSLOADING, cfg.isPeerClassLoadingEnabled());
        add(attrs, ATTR_DEPLOYMENT_MODE, cfg.getDeploymentMode());
        add(attrs, ATTR_LANG_RUNTIME, getLanguage());

        add(attrs, ATTR_JVM_PID, U.jvmPid());

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
        add(attrs, ATTR_JVM_ARGS, jvmArgs.toString());

        // Check daemon system property and override configuration if it's set.
        if (isDaemon())
            add(attrs, ATTR_DAEMON, "true");

        // In case of the parsing error, JMX remote disabled or port not being set
        // node attribute won't be set.
        if (isJmxRemoteEnabled()) {
            String portStr = System.getProperty("com.sun.management.jmxremote.port");

            if (portStr != null)
                try {
                    add(attrs, ATTR_JMX_PORT, Integer.parseInt(portStr));
                }
                catch (NumberFormatException ignore) {
                    // No-op.
                }
        }

        // Whether restart is enabled and stick the attribute.
        add(attrs, ATTR_RESTART_ENABLED, Boolean.toString(isRestartEnabled()));

        // Save port range, port numbers will be stored by rest processor at runtime.
        if (cfg.getClientConnectionConfiguration() != null)
            add(attrs, ATTR_REST_PORT_RANGE, cfg.getClientConnectionConfiguration().getRestPortRange());

        try {
            AuthenticationSpi authSpi = cfg.getAuthenticationSpi();

            boolean securityEnabled = authSpi != null && !U.hasAnnotation(authSpi.getClass(), IgniteSpiNoop.class);

            GridSecurityCredentialsProvider provider = cfg.getSecurityCredentialsProvider();

            if (provider != null) {
                GridSecurityCredentials cred = provider.credentials();

                if (cred != null)
                    add(attrs, ATTR_SECURITY_CREDENTIALS, cred);
                else if (securityEnabled)
                    throw new IgniteCheckedException("Failed to start node (authentication SPI is configured, " +
                        "by security credentials provider returned null).");
            }
            else if (securityEnabled)
                throw new IgniteCheckedException("Failed to start node (authentication SPI is configured, " +
                    "but security credentials provider is not set. Fix the configuration and restart the node).");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to create node security credentials", e);
        }

        // Stick in SPI versions and classes attributes.
        addAttributes(attrs, cfg.getCollisionSpi());
        addAttributes(attrs, cfg.getSwapSpaceSpi());
        addAttributes(attrs, cfg.getDiscoverySpi());
        addAttributes(attrs, cfg.getFailoverSpi());
        addAttributes(attrs, cfg.getCommunicationSpi());
        addAttributes(attrs, cfg.getEventStorageSpi());
        addAttributes(attrs, cfg.getCheckpointSpi());
        addAttributes(attrs, cfg.getLoadBalancingSpi());
        addAttributes(attrs, cfg.getAuthenticationSpi());
        addAttributes(attrs, cfg.getSecureSessionSpi());
        addAttributes(attrs, cfg.getDeploymentSpi());

        // Set user attributes for this node.
        if (cfg.getUserAttributes() != null) {
            for (Map.Entry<String, ?> e : cfg.getUserAttributes().entrySet()) {
                if (attrs.containsKey(e.getKey()))
                    U.warn(log, "User or internal attribute has the same name as environment or system " +
                        "property and will take precedence: " + e.getKey());

                attrs.put(e.getKey(), e.getValue());
            }
        }

        return attrs;
    }

    /**
     * Add SPI version and class attributes into node attributes.
     *
     * @param attrs Node attributes map to add SPI attributes to.
     * @param spiList Collection of SPIs to get attributes from.
     * @throws IgniteCheckedException Thrown if was unable to set up attribute.
     */
    private void addAttributes(Map<String, Object> attrs, IgniteSpi... spiList) throws IgniteCheckedException {
        for (IgniteSpi spi : spiList) {
            Class<? extends IgniteSpi> spiCls = spi.getClass();

            add(attrs, U.spiAttribute(spi, ATTR_SPI_CLASS), spiCls.getName());
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
                IgniteMBean.class);

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
        ClusterNodeMetricsMBean mbean = new ClusterLocalNodeMetrics(ctx.discovery().localNode());

        try {
            locNodeMBean = U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getGridName(),
                "Kernal",
                mbean.getClass().getSimpleName(),
                mbean,
                ClusterNodeMetricsMBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered local node MBean: " + locNodeMBean);
        }
        catch (JMException e) {
            locNodeMBean = null;

            throw new IgniteCheckedException("Failed to register local node MBean.", e);
        }
    }

    /** @throws IgniteCheckedException If registration failed. */
    private void registerExecutorMBeans() throws IgniteCheckedException {
        pubExecSvcMBean = registerExecutorMBean(cfg.getExecutorService(), "GridExecutionExecutor");
        sysExecSvcMBean = registerExecutorMBean(cfg.getSystemExecutorService(), "GridSystemExecutor");
        mgmtExecSvcMBean = registerExecutorMBean(cfg.getManagementExecutorService(), "GridManagementExecutor");
        p2PExecSvcMBean = registerExecutorMBean(cfg.getPeerClassLoadingExecutorService(), "GridClassLoadingExecutor");

        ClientConnectionConfiguration clientCfg = cfg.getClientConnectionConfiguration();

        if (clientCfg != null) {
            restExecSvcMBean = clientCfg.getRestExecutorService() != null ?
                registerExecutorMBean(clientCfg.getRestExecutorService(), "GridRestExecutor") : null;
        }
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
                new IgniteThreadPoolMBeanAdapter(exec),
                IgniteThreadPoolMBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered executor service MBean: " + res);

            return res;
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register executor service MBean [name=" + name + ", exec=" + exec + ']',
                e);
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
     * @param ctx Kernal context.
     * @param mgr Manager to start.
     * @param attrs SPI attributes to set.
     * @throws IgniteCheckedException Throw in case of any errors.
     */
    private void startManager(GridKernalContextImpl ctx, GridManager mgr, Map<String, Object> attrs)
        throws IgniteCheckedException {
        mgr.addSpiAttributes(attrs);

        // Set all node attributes into discovery manager,
        // so they can be distributed to all nodes.
        if (mgr instanceof GridDiscoveryManager)
            ((GridDiscoveryManager)mgr).setNodeAttributes(attrs, ctx.product().version());

        // Add manager to registry before it starts to avoid
        // cases when manager is started but registry does not
        // have it yet.
        ctx.add(mgr);

        try {
            mgr.start();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to start manager: " + mgr, e);
        }
    }

    /**
     * @param ctx Kernal context.
     * @param proc Processor to start.
     * @param attrs Attributes.
     * @throws IgniteCheckedException Thrown in case of any error.
     */
    private void startProcessor(GridKernalContextImpl ctx, GridProcessor proc, Map<String, Object> attrs)
        throws IgniteCheckedException {
        ctx.add(proc);

        try {
            proc.start();

            proc.addAttributes(attrs);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to start processor: " + proc, e);
        }
    }

    /**
     * Add helper.
     *
     * @param ctx Context.
     * @param helper Helper.
     */
    private void addHelper(GridKernalContextImpl ctx, Object helper) {
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

        return cfg.getClientConnectionConfiguration() != null;
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
            log.info("Config URL: " + System.getProperty(GG_CONFIG_URL, "n/a"));
    }

    /**
     * Acks Visor instructions.
     */
    private void ackVisor() {
        assert log != null;

        if (isDaemon())
            return;

        if (ctx.isEnterprise())
            U.quietAndInfo(log, "To start GUI Management & Monitoring run ggvisorui.{sh|bat}");
        else
            U.quietAndInfo(log, "To start Console Management & Monitoring run ggvisorcmd.{sh|bat}");
    }

    /**
     * Acks benchmarking instructions.
     */
    private void ackBenchmarks() {
        if (!isDaemon())
            U.quietAndInfo(log, "If running benchmarks, see http://bit.ly/GridGain-Benchmarking");
    }

    /**
     * Acks ASCII-logo. Thanks to http://patorjk.com/software/taag
     */
    private void ackAsciiLogo() {
        assert log != null;

        String fileName = log.fileName();

        if (System.getProperty(GG_NO_ASCII) == null) {
            String ver = "ver. " + ACK_VER;

            // Big thanks to: http://patorjk.com/software/taag
            // Font name "Small Slant"
            if (log.isQuiet()) {
                U.quiet(false,
                    "   __________  ________________ ",
                    "  /  _/ ___/ |/ /  _/_  __/ __/ ",
                    " _/ // (_ /    // /  / / / _/   ",
                    "/___/\\___/_/|_/___/ /_/ /___/  ",
                    " ",
                    ver,
                    COPYRIGHT,
                    "",
                    "Quiet mode.");

                if (fileName != null)
                    U.quiet(false, "  ^-- Logging to file '" +  fileName + '\'');

                U.quiet(false,
                    "  ^-- To see **FULL** console log here add -DGRIDGAIN_QUIET=false or \"-v\" to ggstart.{sh|bat}",
                    "");
            }

            if (log.isInfoEnabled()) {
                log.info(NL + NL +
                    ">>>    __________  ________________  " + NL +
                    ">>>   /  _/ ___/ |/ /  _/_  __/ __/  " + NL +
                    ">>>  _/ // (_ /    // /  / / / _/    " + NL +
                    ">>> /___/\\___/_/|_/___/ /_/ /___/   " + NL +
                    ">>> " + NL +
                    ">>> " + ver + NL +
                    ">>> " + COPYRIGHT + NL
                );
            }
        }
    }

    /**
     * Prints start info.
     *
     * @param rtBean Java runtime bean.
     */
    private void ackStart(RuntimeMXBean rtBean) {
        if (log.isQuiet()) {
            U.quiet(false, "");
            U.quiet(false, "GridGain node started OK (id=" + U.id8(localNode().id()) +
                (F.isEmpty(gridName) ? "" : ", grid=" + gridName) + ')');
        }

        if (log.isInfoEnabled()) {
            log.info("");

            String ack = "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH;

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
                    ">>> CPU(s): " + localNode().metrics().getTotalCpus() + NL +
                    ">>> Heap: " + U.heapSize(localNode(), 2) + "GB" + NL +
                    ">>> VM name: " + rtBean.getName() + NL +
                    ">>> Grid name: " + gridName + NL +
                    ">>> Local node [" +
                    "ID=" + localNode().id().toString().toUpperCase() +
                    ", order=" + localNode().order() +
                    "]" + NL +
                    ">>> Local node addresses: " + U.addressesAsString(localNode()) + NL +
                    ">>> Local ports: " + sb + NL;

            str += ">>> GridGain documentation: http://" + SITE + "/documentation" + NL;

            log.info(str);
        }
    }

    /**
     * Logs out OS information.
     */
    private void ackOsInfo() {
        assert log != null;

        if (log.isInfoEnabled()) {
            log.info("OS: " + U.osString());
            log.info("OS user: " + System.getProperty("user.name"));
        }
    }

    /**
     * Logs out language runtime.
     */
    private void ackLanguageRuntime() {
        assert log != null;

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
            catch (Throwable ignore) {
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
     * @param cancel Whether or not to cancel running jobs.
     */
    private void stop0(boolean cancel) {
        String nid = getLocalNodeId().toString().toUpperCase();
        String nid8 = U.id8(getLocalNodeId()).toUpperCase();

        gw.compareAndSet(null, new GridKernalGatewayImpl(gridName));

        GridKernalGateway gw = this.gw.get();

        if (stopGuard.compareAndSet(false, true)) {
            // Only one thread is allowed to perform stop sequence.
            boolean firstStop = false;

            GridKernalState state = gw.getState();

            if (state == STARTED)
                firstStop = true;
            else if (state == STARTING)
                U.warn(log, "Attempt to stop starting grid. This operation " +
                    "cannot be guaranteed to be successful.");

            if (firstStop) {
                // Notify lifecycle beans.
                if (log.isDebugEnabled())
                    log.debug("Notifying lifecycle beans.");

                notifyLifecycleBeansEx(LifecycleEventType.BEFORE_GRID_STOP);
            }

            GridEmailProcessorAdapter email = ctx.email();

            List<GridComponent> comps = ctx.components();

            // Callback component in reverse order while kernal is still functional
            // if called in the same thread, at least.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
                GridComponent comp = it.previous();

                try {
                    comp.onKernalStop(cancel);
                }
                catch (Throwable e) {
                    errOnStop = true;

                    U.error(log, "Failed to pre-stop processor: " + comp, e);
                }
            }

            gw.writeLock();

            try {
                assert gw.getState() == STARTED || gw.getState() == STARTING;

                // No more kernal calls from this point on.
                gw.setState(STOPPING);

                // Cancel update notification timer.
                if (updateNtfTimer != null)
                    updateNtfTimer.cancel();

                if (starveTimer != null)
                    starveTimer.cancel();

                // Cancel license timer.
                if (licTimer != null)
                    licTimer.cancel();

                // Cancel metrics log timer.
                if (metricsLogTimer != null)
                    metricsLogTimer.cancel();

                // Clear node local store.
                nodeLoc.clear();

                if (log.isDebugEnabled())
                    log.debug("Grid " + (gridName == null ? "" : '\'' + gridName + "' ") + "is stopping.");
            }
            finally {
                gw.writeUnlock();
            }

            // Unregister MBeans.
            if (!(
                unregisterMBean(pubExecSvcMBean) &
                    unregisterMBean(sysExecSvcMBean) &
                    unregisterMBean(mgmtExecSvcMBean) &
                    unregisterMBean(p2PExecSvcMBean) &
                    unregisterMBean(kernalMBean) &
                    unregisterMBean(locNodeMBean) &
                    unregisterMBean(restExecSvcMBean)
            ))
                errOnStop = false;

            // Stop components in reverse order.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
                GridComponent comp = it.previous();

                try {
                    comp.stop(cancel);

                    if (log.isDebugEnabled())
                        log.debug("Component stopped: " + comp);
                }
                catch (Throwable e) {
                    errOnStop = true;

                    U.error(log, "Failed to stop component (ignoring): " + comp, e);
                }
            }

            // Stops lifecycle aware components.
            U.stopLifecycleAware(log, lifecycleAwares(cfg));

            // Lifecycle notification.
            notifyLifecycleBeansEx(LifecycleEventType.AFTER_GRID_STOP);

            // Clean internal class/classloader caches to avoid stopped contexts held in memory.
            IgniteOptimizedMarshaller.clearCache();
            IgniteMarshallerExclusions.clearCache();
            GridEnumCache.clear();

            gw.writeLock();

            try {
                gw.setState(STOPPED);
            }
            finally {
                gw.writeUnlock();
            }

            // Ack stop.
            if (log.isQuiet()) {
                if (!errOnStop)
                    U.quiet(false, "GridGain node stopped OK [uptime=" +
                        X.timeSpan2HMSM(U.currentTimeMillis() - startTime) + ']');
                else
                    U.quiet(true, "GridGain node stopped wih ERRORS [uptime=" +
                        X.timeSpan2HMSM(U.currentTimeMillis() - startTime) + ']');
            }

            if (log.isInfoEnabled())
                if (!errOnStop) {
                    String ack = "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH +
                        " stopped OK";

                    String dash = U.dash(ack.length());

                    log.info(NL + NL +
                        ">>> " + dash + NL +
                        ">>> " + ack + NL +
                        ">>> " + dash + NL +
                        ">>> Grid name: " + gridName + NL +
                        ">>> Grid uptime: " + X.timeSpan2HMSM(U.currentTimeMillis() - startTime) +
                        NL +
                        NL);
                }
                else {
                    String ack = "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH +
                        " stopped with ERRORS";

                    String dash = U.dash(ack.length());

                    log.info(NL + NL +
                        ">>> " + ack + NL +
                        ">>> " + dash + NL +
                        ">>> Grid name: " + gridName + NL +
                        ">>> Grid uptime: " + X.timeSpan2HMSM(U.currentTimeMillis() - startTime) +
                        NL +
                        ">>> See log above for detailed error message." + NL +
                        ">>> Note that some errors during stop can prevent grid from" + NL +
                        ">>> maintaining correct topology since this node may have" + NL +
                        ">>> not exited grid properly." + NL +
                        NL);
                }

            // Send node start email notification, if enabled.
            if (isSmtpEnabled() && isAdminEmailsSet() && cfg.isLifeCycleEmailNotification()) {
                String errOk = errOnStop ? "with ERRORS" : "OK";

                String headline = "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR +
                    " stopped " + errOk + ":";
                String subj = "GridGain node stopped " + errOk + ": " + nid8;

                IgniteProductLicense lic = ctx.license() != null ? ctx.license().license() : null;

                String body =
                    headline + NL + NL +
                    "----" + NL +
                    "GridGain ver. " + COMPOUND_VER + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH + NL +
                    "Grid name: " + gridName + NL +
                    "Node ID: " + nid + NL +
                    "Node uptime: " + X.timeSpan2HMSM(U.currentTimeMillis() - startTime) + NL;

                if (lic != null) {
                    body +=
                        "License ID: " + lic.id().toString().toUpperCase() + NL +
                        "Licensed to: " + lic.userOrganization() + NL;
                }
                else
                    assert !ENT;

                body +=
                    "----" + NL +
                    NL +
                    "NOTE:" + NL +
                    "This message is sent automatically to all configured admin emails." + NL +
                    "To change this behavior use 'lifeCycleEmailNotify' grid configuration property.";

                if (errOnStop)
                    body +=
                        NL + NL +
                            "NOTE:" + NL +
                            "See node's log for detailed error message." + NL +
                            "Some errors during stop can prevent grid from" + NL +
                            "maintaining correct topology since this node may " + NL +
                            "have not exited grid properly.";

                body +=
                    NL + NL +
                        "| " + SITE + NL +
                        "| support@gridgain.com" + NL;

                if (email != null) {
                    try {
                        email.sendNow(subj,
                            body,
                            false,
                            Arrays.asList(cfg.getAdminEmails()));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send lifecycle email notification.", e);
                    }
                }
            }

            U.onGridStop();
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
    public GridKernalContext context() {
        return ctx;
    }

    /**
     * Prints all system properties in debug mode.
     */
    private void ackSystemProperties() {
        assert log != null;

        if (log.isDebugEnabled())
            for (Object key : U.asIterable(System.getProperties().keys()))
                log.debug("System property [" + key + '=' + System.getProperty((String) key) + ']');
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

        return cfg.isDaemon() || "true".equalsIgnoreCase(System.getProperty(GG_DAEMON));
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
     * with {@code bin/ggstart.{sh|bat}} script using {@code -r} argument. Node can be
     * programmatically restarted using {@link org.apache.ignite.Ignition#restart(boolean)}} method.
     *
     * @return {@code True} if restart mode is enabled, {@code false} otherwise.
     * @see org.apache.ignite.Ignition#restart(boolean)
     */
    @Override public boolean isRestartEnabled() {
        return System.getProperty(GG_SUCCESS_FILE) != null;
    }

    /**
     * Whether or not SMTP is configured. Note that SMTP is considered configured if
     * SMTP host is provided in configuration (see {@link org.apache.ignite.configuration.IgniteConfiguration#getSmtpHost()}.
     * <p>
     * If SMTP is not configured all emails notifications will be disabled.
     *
     * @return {@code True} if SMTP is configured - {@code false} otherwise.
     * @see org.apache.ignite.configuration.IgniteConfiguration#getSmtpFromEmail()
     * @see org.apache.ignite.configuration.IgniteConfiguration#getSmtpHost()
     * @see org.apache.ignite.configuration.IgniteConfiguration#getSmtpPassword()
     * @see org.apache.ignite.configuration.IgniteConfiguration#getSmtpPort()
     * @see org.apache.ignite.configuration.IgniteConfiguration#getSmtpUsername()
     * @see org.apache.ignite.configuration.IgniteConfiguration#isSmtpSsl()
     * @see org.apache.ignite.configuration.IgniteConfiguration#isSmtpStartTls()
     * @see #sendAdminEmailAsync(String, String, boolean)
     */
    @Override public boolean isSmtpEnabled() {
        assert cfg != null;

        return cfg.getSmtpHost() != null;
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
            log.debug("Grid authentication SPI : " + cfg.getAuthenticationSpi());
            log.debug("Grid secure session SPI : " + cfg.getSecureSessionSpi());
            log.debug("Grid swap space SPI     : " + cfg.getSwapSpaceSpi());
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
                String name = c.getName();

                if (name == null)
                    name = "<default>";

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
     *
     * @param ctx Kernal context.
     */
    private void ackSecurity(GridKernalContext ctx) {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Security status [authentication=" + onOff(ctx.security().enabled()) + ", " +
                "secure-session=" + onOff(ctx.secureSession().enabled()) + ']');
    }

    /**
     * Prints out SMTP configuration.
     */
    private void ackSmtpConfiguration() {
        assert log != null;

        String host = cfg.getSmtpHost();

        boolean ssl = cfg.isSmtpSsl();
        int port = cfg.getSmtpPort();

        if (host == null) {
            U.warn(log, "SMTP is not configured - email notifications are off.");

            return;
        }

        String from = cfg.getSmtpFromEmail();

        if (log.isQuiet())
            U.quiet(false, "SMTP enabled [host=" + host + ":" + port + ", ssl=" + (ssl ? "on" : "off") + ", from=" +
                from + ']');

        if (log.isInfoEnabled()) {
            String[] adminEmails = cfg.getAdminEmails();

            log.info("SMTP enabled [host=" + host + ", port=" + port + ", ssl=" + ssl + ", from=" + from + ']');
            log.info("Admin emails: " + (!isAdminEmailsSet() ? "N/A" : Arrays.toString(adminEmails)));
        }

        if (!isAdminEmailsSet())
            U.warn(log, "Admin emails are not set - automatic email notifications are off.");
    }

    /**
     * Tests whether or not admin emails are set.
     *
     * @return {@code True} if admin emails are set and not empty.
     */
    private boolean isAdminEmailsSet() {
        assert cfg != null;

        String[] a = cfg.getAdminEmails();

        return a != null && a.length > 0;
    }

    /**
     * Prints out VM arguments and GRIDGAIN_HOME in info mode.
     *
     * @param rtBean Java runtime bean.
     */
    private void ackVmArguments(RuntimeMXBean rtBean) {
        assert log != null;

        // Ack GRIDGAIN_HOME and VM arguments.
        if (log.isInfoEnabled()) {
            log.info("GRIDGAIN_HOME=" + cfg.getGridGainHome());
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
     * @return Components provided in configuration which can implement {@link org.apache.ignite.lifecycle.LifecycleAware} interface.
     */
    private Iterable<Object> lifecycleAwares(IgniteConfiguration cfg) {
        Collection<Object> objs = new ArrayList<>();

        if (!F.isEmpty(cfg.getLifecycleBeans()))
            F.copy(objs, cfg.getLifecycleBeans());

        if (!F.isEmpty(cfg.getSegmentationResolvers()))
            F.copy(objs, cfg.getSegmentationResolvers());

        if (cfg.getClientConnectionConfiguration() != null)
            F.copy(objs, cfg.getClientConnectionConfiguration().getClientMessageInterceptor(),
                cfg.getClientConnectionConfiguration().getRestTcpSslContextFactory());

        F.copy(objs, cfg.getMarshaller(), cfg.getGridLogger(), cfg.getMBeanServer());

        return objs;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return log;
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

        return pingNode(UUID.fromString(nodeId));
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> topology(long topVer) {
        guard();

        try {
            return ctx.discovery().topology(topVer);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        guard();

        try {
            return ctx.discovery().topologyVersion();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void undeployTaskFromGrid(String taskName) throws JMException {
        A.notNull(taskName, "taskName");

        try {
            compute().undeployTask(taskName);
        }
        catch (IgniteCheckedException e) {
            throw U.jmException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public String executeTask(String taskName, String arg) throws JMException {
        try {
            return compute().<String, String>execute(taskName, arg);
        }
        catch (IgniteCheckedException e) {
            throw U.jmException(e);
        }
    }

    /**
     * Schedule sending of given email to all configured admin emails. If no admin emails are configured this
     * method is no-op. If SMTP is not configured this method is no-op.
     * <p>
     * Note that this method returns immediately with the future and all emails will be sent asynchronously
     * in a different thread. If email queue is full or sending has failed - the email will be lost.
     * Email queue can fill up if rate of scheduling emails is greater than the rate of SMTP sending.
     * <p>
     * Implementation is not performing any throttling and it is responsibility of the caller to properly
     * throttle the emails, if necessary.
     *
     * @param subj Subject of the email.
     * @param body Body of the email.
     * @param html If {@code true} the email body will have MIME {@code html} subtype.
     * @return Email's future. You can use this future to check on the status of the email. If future
     *      completes ok and its result value is {@code true} email was successfully sent. In all
     *      other cases - sending process has failed.
     * @see #isSmtpEnabled()
     * @see org.apache.ignite.configuration.IgniteConfiguration#getAdminEmails()
     */
    @Override public IgniteFuture<Boolean> sendAdminEmailAsync(String subj, String body, boolean html) {
        A.notNull(subj, "subj");
        A.notNull(body, "body");

        if (isSmtpEnabled() && isAdminEmailsSet()) {
            guard();

            try {
                return ctx.email().schedule(subj, body, html, Arrays.asList(cfg.getAdminEmails()));
            }
            finally {
                unguard();
            }
        }
        else
            return new GridFinishedFuture<>(ctx, false);
    }

    /** {@inheritDoc} */
    @Override public boolean pingNodeByAddress(String host) {
        guard();

        try {
            for (ClusterNode n : nodes())
                if (n.addresses().contains(host))
                    return ctx.discovery().pingNode(n.id());

            return false;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        guard();

        try {
            ClusterNode node = ctx.discovery().localNode();

            assert node != null;

            return node;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> ClusterNodeLocalMap<K, V> nodeLocalMap() {
        guard();

        try {
            return nodeLoc;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        guard();

        try {
            return ctx.discovery().pingNode(nodeId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(File file, boolean restart,
        int timeout, int maxConn) throws IgniteCheckedException {
        return startNodesAsync(file, restart, timeout, maxConn).get();
    }

    /**
     * @param file Configuration file.
     * @param restart Whether to stop existing nodes.
     * @param timeout Connection timeout.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future with results.
     * @throws IgniteCheckedException In case of error.
     * @see {@link org.apache.ignite.IgniteCluster#startNodes(java.io.File, boolean, int, int)}.
     */
    IgniteFuture<Collection<GridTuple3<String, Boolean, String>>> startNodesAsync(File file, boolean restart,                                                                                            int timeout, int maxConn) throws IgniteCheckedException {
        A.notNull(file, "file");
        A.ensure(file.exists(), "file doesn't exist.");
        A.ensure(file.isFile(), "file is a directory.");

        IgniteBiTuple<Collection<Map<String, Object>>, Map<String, Object>> t = parseFile(file);

        return startNodesAsync(t.get1(), t.get2(), restart, timeout, maxConn);
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster enableAsync() {
        return new IgniteClusterAsyncImpl(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new IllegalStateException("Asynchronous mode is not enabled.");
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
        int maxConn) throws IgniteCheckedException {
        return startNodesAsync(hosts, dflts, restart, timeout, maxConn).get();
    }

    /**
     * @param hosts Startup parameters.
     * @param dflts Default values.
     * @param restart Whether to stop existing nodes
     * @param timeout Connection timeout in milliseconds.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future with results.
     * @throws IgniteCheckedException In case of error.
     * @see {@link org.apache.ignite.IgniteCluster#startNodes(java.util.Collection, java.util.Map, boolean, int, int)}.
     */
    IgniteFuture<Collection<GridTuple3<String, Boolean, String>>> startNodesAsync(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
        int maxConn) throws IgniteCheckedException {
        A.notNull(hosts, "hosts");

        guard();

        try {
            GridSshProcessor sshProcessor = GridComponentType.SSH.create(false);

            Map<String, Collection<GridRemoteStartSpecification>> specsMap = specifications(hosts, dflts);

            Map<String, ConcurrentLinkedQueue<GridNodeCallable>> runMap = new HashMap<>();

            int nodeCallCnt = 0;

            for (String host : specsMap.keySet()) {
                InetAddress addr;

                try {
                    addr = InetAddress.getByName(host);
                }
                catch (UnknownHostException e) {
                    throw new IgniteCheckedException("Invalid host name: " + host, e);
                }

                Collection<? extends ClusterNode> neighbors = null;

                if (addr.isLoopbackAddress())
                    neighbors = neighbors();
                else {
                    for (Collection<ClusterNode> p : U.neighborhood(nodes()).values()) {
                        ClusterNode node = F.first(p);

                        if (node.<String>attribute(ATTR_IPS).contains(addr.getHostAddress())) {
                            neighbors = p;

                            break;
                        }
                    }
                }

                int startIdx = 1;

                if (neighbors != null) {
                    if (restart && !neighbors.isEmpty()) {
                        try {
                            compute(forNodes(neighbors)).execute(GridKillTask.class, false);
                        }
                        catch (ClusterGroupEmptyException ignored) {
                            // No-op, nothing to restart.
                        }
                    }
                    else
                        startIdx = neighbors.size() + 1;
                }

                ConcurrentLinkedQueue<GridNodeCallable> nodeRuns = new ConcurrentLinkedQueue<>();

                runMap.put(host, nodeRuns);

                for (GridRemoteStartSpecification spec : specsMap.get(host)) {
                    assert spec.host().equals(host);

                    for (int i = startIdx; i <= spec.nodes(); i++) {
                        nodeRuns.add(sshProcessor.nodeStartCallable(spec, timeout));

                        nodeCallCnt++;
                    }
                }
            }

            // If there is nothing to start, return finished future with empty result.
            if (nodeCallCnt == 0)
                return new GridFinishedFuture<Collection<GridTuple3<String, Boolean, String>>>(
                    ctx, Collections.<GridTuple3<String, Boolean, String>>emptyList());

            // Exceeding max line width for readability.
            GridCompoundFuture<GridTuple3<String, Boolean, String>, Collection<GridTuple3<String, Boolean, String>>>
                fut = new GridCompoundFuture<>(
                    ctx,
                    CU.<GridTuple3<String, Boolean, String>>objectsReducer()
                );

            AtomicInteger cnt = new AtomicInteger(nodeCallCnt);

            // Limit maximum simultaneous connection number per host.
            for (ConcurrentLinkedQueue<GridNodeCallable> queue : runMap.values()) {
                for (int i = 0; i < maxConn; i++) {
                    if (!runNextNodeCallable(queue, fut, cnt))
                        break;
                }
            }

            return fut;
        }
        finally {
            unguard();
        }
    }

    /**
     * Gets the all grid nodes that reside on the same physical computer as local grid node.
     * Local grid node is excluded.
     * <p>
     * Detection of the same physical computer is based on comparing set of network interface MACs.
     * If two nodes have the same set of MACs, GridGain considers these nodes running on the same
     * physical computer.
     * @return Grid nodes that reside on the same physical computer as local grid node.
     */
    private Collection<ClusterNode> neighbors() {
        Collection<ClusterNode> neighbors = new ArrayList<>(1);

        String macs = localNode().attribute(ATTR_MACS);

        assert macs != null;

        for (ClusterNode n : forOthers(localNode()).nodes()) {
            if (macs.equals(n.attribute(ATTR_MACS)))
                neighbors.add(n);
        }

        return neighbors;
    }

    /**
     * Runs next callable from host node start queue.
     *
     * @param queue Queue of tasks to poll from.
     * @param comp Compound future that comprise all started node tasks.
     * @param cnt Atomic counter to check if all futures are added to compound future.
     * @return {@code True} if task was started, {@code false} if queue was empty.
     */
    private boolean runNextNodeCallable(final ConcurrentLinkedQueue<GridNodeCallable> queue,
        final GridCompoundFuture<GridTuple3<String, Boolean, String>,
        Collection<GridTuple3<String, Boolean, String>>> comp, final AtomicInteger cnt) {
        GridNodeCallable call = queue.poll();

        if (call == null)
            return false;

        IgniteFuture<GridTuple3<String, Boolean, String>> fut = ctx.closure().callLocalSafe(call, true);

        comp.add(fut);

        if (cnt.decrementAndGet() == 0)
            comp.markInitialized();

        fut.listenAsync(new CI1<IgniteFuture<GridTuple3<String, Boolean, String>>>() {
            @Override public void apply(IgniteFuture<GridTuple3<String, Boolean, String>> f) {
                runNextNodeCallable(queue, comp, cnt);
            }
        });

        return true;
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws IgniteCheckedException {
        guard();

        try {
            compute().execute(GridKillTask.class, false);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws IgniteCheckedException {
        guard();

        try {
            compute(forNodeIds(ids)).execute(GridKillTask.class, false);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws IgniteCheckedException {
        guard();

        try {
            compute().execute(GridKillTask.class, true);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws IgniteCheckedException {
        guard();

        try {
            compute(forNodeIds(ids)).execute(GridKillTask.class, true);
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
            if (!dbUsageRegistered) {
                GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

                dbUsageRegistered = true;
            }

            return ctx.cache().transactions();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cache(@Nullable String name) {
        guard();

        try {
            if (!dbUsageRegistered) {
                GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

                dbUsageRegistered = true;
            }

            return ctx.cache().publicCache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> jcache(@Nullable String name) {
        guard();

        try {
            if (!dbUsageRegistered) {
                GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

                dbUsageRegistered = true;
            }

            return ctx.cache().publicJCache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCache<?, ?>> caches() {
        guard();

        try {
            if (!dbUsageRegistered) {
                GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

                dbUsageRegistered = true;
            }

            return ctx.cache().publicCaches();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K extends GridCacheUtilityKey, V> GridCacheProjectionEx<K, V> utilityCache(Class<K> keyCls,
        Class<V> valCls) {
        guard();

        try {
            return ctx.cache().utilityCache(keyCls, valCls);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cachex(@Nullable String name) {
        guard();

        try {
            return ctx.cache().cache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cachex() {
        guard();

        try {
            return ctx.cache().cache();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCache<?, ?>> cachesx(IgnitePredicate<? super GridCache<?, ?>>[] p) {
        guard();

        try {
            return F.retain(ctx.cache().caches(), true, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataLoader<K, V> dataLoader(@Nullable String cacheName) {
        guard();

        try {
            if (!dbUsageRegistered) {
                GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

                dbUsageRegistered = true;
            }

            return ctx.<K, V>dataLoad().dataLoader(cacheName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFs fileSystem(String name) {
        guard();

        try{
            IgniteFs ggfs = ctx.ggfs().ggfs(name);

            if (ggfs == null)
                throw new IllegalArgumentException("GGFS is not configured: " + name);

            return ggfs;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFs ggfsx(@Nullable String name) {
        guard();

        try {
            return ctx.ggfs().ggfs(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFs> fileSystems() {
        guard();

        try {
            return ctx.ggfs().ggfss();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoop hadoop() {
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
    @Override public GridInteropProcessor interop() {
        return ctx.interop();
    }

    /** {@inheritDoc} */
    @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(String cacheName,
        @Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        guard();

        try {
            return ctx.affinity().mapKeysToNodes(cacheName, keys);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> ClusterNode mapKeyToNode(String cacheName, K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        guard();

        try {
            return ctx.affinity().mapKeyToNode(cacheName, key);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        guard();

        try {
            ctx.jobMetric().reset();
            ctx.io().resetMetrics();
            ctx.task().resetMetrics();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteStreamer streamer(@Nullable String name) {
        guard();

        try {
            return ctx.stream().streamer(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteStreamer> streamers() {
        guard();

        try {
            return ctx.stream().streamers();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forLocal() {
        ctx.gateway().readLock();

        try {
            return new ClusterGroupAdapter(this, ctx, null, Collections.singleton(cfg.getNodeId()));
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteProduct product() {
        return ctx.product();
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return scheduler;
    }

    /** {@inheritDoc} */
    @Override public GridSecurity security() {
        if (!ctx.isEnterprise())
            throw new UnsupportedOperationException("Security interface available in Enterprise edition only.");

        return security;
    }

    /** {@inheritDoc} */
    @Override public IgnitePortables portables() {
        if (!ctx.isEnterprise())
            throw new UnsupportedOperationException("Portables interface available in Enterprise edition only.");

        return portables;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> compatibleVersions() {
        return compatibleVers;
    }

    /** {@inheritDoc} */
    @Override public long licenseGracePeriodLeft() {
        return ctx.license().gracePeriodLeft();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        Ignition.stop(gridName, true);
    }

    /**
     * Creates optional component.
     *
     * @param cls Component interface.
     * @param ctx Kernal context.
     * @return Created component.
     * @throws IgniteCheckedException If failed to create component.
     */
    private static <T extends GridComponent> T createComponent(Class<T> cls, GridKernalContext ctx) throws IgniteCheckedException {
        assert cls.isInterface() : cls;

        T comp = ctx.plugins().createComponent(cls);

        if (comp != null)
            return comp;

        // TODO 9341: get rid of ent/os after moving ent code to plugin.
        Class<T> implCls = null;

        try {
            implCls = (Class<T>)Class.forName(enterpriseClassName(cls));
        }
        catch (ClassNotFoundException ignore) {
            // No-op.
        }

        if (implCls == null) {
            try {
                implCls = (Class<T>)Class.forName(openSourceClassName(cls));
            }
            catch (ClassNotFoundException ignore) {
                // No-op.
            }
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
     * @return Name of component implementation class for enterprise edition.
     */
    private static String enterpriseClassName(Class<?> cls) {
        return cls.getPackage().getName() + ".ent." + cls.getSimpleName().replace("Grid", "GridEnt");
    }

    /**
     * @param cls Component interface.
     * @return Name of component implementation class for open source edition.
     */
    private static String openSourceClassName(Class<?> cls) {
        return cls.getPackage().getName() + ".os." + cls.getSimpleName().replace("Grid", "GridOs");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKernal.class, this);
    }
}
