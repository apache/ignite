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
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
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
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.clock.GridClockSource;
import org.apache.ignite.internal.processors.clock.GridClockSyncProcessor;
import org.apache.ignite.internal.processors.clock.GridJvmClockSource;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.hadoop.HadoopProcessorAdapter;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.processors.igfs.IgfsHelper;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.odbc.OdbcProcessor;
import org.apache.ignite.internal.processors.offheap.GridOffHeapProcessor;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.schedule.IgniteScheduleProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.session.GridTaskSessionProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DAEMON;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Implementation of kernal context.
 */
@GridToStringExclude
public class GridKernalContextImpl implements GridKernalContext, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ThreadLocal<String> stash = new ThreadLocal<>();

    /*
     * Managers.
     * ========
     */

    /** */
    @GridToStringExclude
    private GridDeploymentManager depMgr;

    /** */
    @GridToStringExclude
    private GridIoManager ioMgr;

    /** */
    @GridToStringExclude
    private GridDiscoveryManager discoMgr;

    /** */
    @GridToStringExclude
    private GridCheckpointManager cpMgr;

    /** */
    @GridToStringExclude
    private GridEventStorageManager evtMgr;

    /** */
    @GridToStringExclude
    private GridFailoverManager failoverMgr;

    /** */
    @GridToStringExclude
    private GridCollisionManager colMgr;

    /** */
    @GridToStringExclude
    private GridLoadBalancerManager loadMgr;

    /** */
    @GridToStringExclude
    private GridSecurityProcessor authProc;

    /** */
    @GridToStringExclude
    private GridSwapSpaceManager swapspaceMgr;

    /** */
    @GridToStringExclude
    private GridIndexingManager indexingMgr;

    /*
     * Processors.
     * ==========
     */

    /** */
    @GridToStringInclude
    private OdbcProcessor odbcProc;

    /** */
    @GridToStringInclude
    private GridQueryProcessor qryProc;

    /** */
    @GridToStringInclude
    private GridTaskProcessor taskProc;

    /** */
    @GridToStringInclude
    private GridJobProcessor jobProc;

    /** */
    @GridToStringInclude
    private GridTimeoutProcessor timeProc;

    /** */
    @GridToStringInclude
    private GridClockSyncProcessor clockSyncProc;

    /** */
    @GridToStringInclude
    private GridResourceProcessor rsrcProc;

    /** */
    @GridToStringInclude
    private GridJobMetricsProcessor metricsProc;

    /** */
    @GridToStringInclude
    private GridClosureProcessor closProc;

    /** */
    @GridToStringInclude
    private GridServiceProcessor svcProc;

    /** */
    @GridToStringInclude
    private GridCacheProcessor cacheProc;

    /** */
    @GridToStringInclude
    private GridTaskSessionProcessor sesProc;

    /** */
    @GridToStringInclude
    private GridPortProcessor portProc;

    /** */
    @GridToStringInclude
    private GridOffHeapProcessor offheapProc;

    /** */
    @GridToStringInclude
    private IgniteScheduleProcessorAdapter scheduleProc;

    /** */
    @GridToStringInclude
    private GridRestProcessor restProc;

    /** */
    @GridToStringInclude
    private DataStreamProcessor dataLdrProc;

    /** */
    @GridToStringInclude
    private IgfsProcessorAdapter igfsProc;

    /** */
    @GridToStringInclude
    private IgfsHelper igfsHelper;

    /** */
    @GridToStringInclude
    private HadoopHelper hadoopHelper;

    /** */
    @GridToStringInclude
    private GridSegmentationProcessor segProc;

    /** */
    @GridToStringInclude
    private GridAffinityProcessor affProc;

    /** */
    @GridToStringExclude
    private GridContinuousProcessor contProc;

    /** */
    @GridToStringExclude
    private HadoopProcessorAdapter hadoopProc;

    /** */
    @GridToStringExclude
    private PoolProcessor poolProc;

    /** */
    @GridToStringExclude
    private IgnitePluginProcessor pluginProc;

    /** */
    @GridToStringExclude
    private IgniteCacheObjectProcessor cacheObjProc;

    /** */
    @GridToStringExclude
    private PlatformProcessor platformProc;

    /** */
    @GridToStringExclude
    private IgniteSpringHelper spring;

    /** */
    @GridToStringExclude
    private ClusterProcessor cluster;

    /** */
    @GridToStringExclude
    private DataStructuresProcessor dataStructuresProc;

    /** */
    @GridToStringExclude
    private List<GridComponent> comps = new LinkedList<>();

    /** */
    @GridToStringExclude
    protected ExecutorService execSvc;

    /** */
    @GridToStringExclude
    protected ExecutorService sysExecSvc;

    /** */
    @GridToStringExclude
    protected StripedExecutor stripedExecSvc;

    /** */
    @GridToStringExclude
    private ExecutorService p2pExecSvc;

    /** */
    @GridToStringExclude
    private ExecutorService mgmtExecSvc;

    /** */
    @GridToStringExclude
    private ExecutorService igfsExecSvc;

    /** */
    @GridToStringExclude
    protected ExecutorService restExecSvc;

    /** */
    @GridToStringExclude
    protected ExecutorService affExecSvc;

    /** */
    @GridToStringExclude
    protected ExecutorService idxExecSvc;

    /** */
    @GridToStringExclude
    protected IgniteStripedThreadPoolExecutor callbackExecSvc;

    /** */
    @GridToStringExclude
    private Map<String, Object> attrs = new HashMap<>();

    /** */
    private IgniteEx grid;

    /** */
    private ExecutorService utilityCachePool;

    /** */
    private ExecutorService marshCachePool;

    /** */
    private IgniteConfiguration cfg;

    /** */
    private GridKernalGateway gw;

    /** Network segmented flag. */
    private volatile boolean segFlag;

    /** Time source. */
    private GridClockSource clockSrc = new GridJvmClockSource();

    /** Performance suggestions. */
    private final GridPerformanceSuggestions perf = new GridPerformanceSuggestions();

    /** Marshaller context. */
    private MarshallerContextImpl marshCtx;

    /** */
    private ClusterNode locNode;

    /** */
    private volatile boolean disconnected;

    /**
     * No-arg constructor is required by externalization.
     */
    public GridKernalContextImpl() {
        // No-op.
    }

    /**
     * Creates new kernal context.
     *
     * @param log Logger.
     * @param grid Grid instance managed by kernal.
     * @param cfg Grid configuration.
     * @param gw Kernal gateway.
     * @param utilityCachePool Utility cache pool.
     * @param marshCachePool Marshaller cache pool.
     * @param execSvc Public executor service.
     * @param sysExecSvc System executor service.
     * @param stripedExecSvc Striped executor.
     * @param p2pExecSvc P2P executor service.
     * @param mgmtExecSvc Management executor service.
     * @param igfsExecSvc IGFS executor service.
     * @param restExecSvc REST executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param plugins Plugin providers.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    protected GridKernalContextImpl(
        GridLoggerProxy log,
        IgniteEx grid,
        IgniteConfiguration cfg,
        GridKernalGateway gw,
        ExecutorService utilityCachePool,
        ExecutorService marshCachePool,
        ExecutorService execSvc,
        ExecutorService sysExecSvc,
        StripedExecutor stripedExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        ExecutorService igfsExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        IgniteStripedThreadPoolExecutor callbackExecSvc,
        List<PluginProvider> plugins
    ) throws IgniteCheckedException {
        assert grid != null;
        assert cfg != null;
        assert gw != null;

        this.grid = grid;
        this.cfg = cfg;
        this.gw = gw;
        this.utilityCachePool = utilityCachePool;
        this.marshCachePool = marshCachePool;
        this.execSvc = execSvc;
        this.sysExecSvc = sysExecSvc;
        this.stripedExecSvc = stripedExecSvc;
        this.p2pExecSvc = p2pExecSvc;
        this.mgmtExecSvc = mgmtExecSvc;
        this.igfsExecSvc = igfsExecSvc;
        this.restExecSvc = restExecSvc;
        this.affExecSvc = affExecSvc;
        this.idxExecSvc = idxExecSvc;
        this.callbackExecSvc = callbackExecSvc;

        String workDir = U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome());

        marshCtx = new MarshallerContextImpl(workDir, plugins);

        try {
            spring = SPRING.create(false);
        }
        catch (IgniteCheckedException ignored) {
            if (log != null && log.isDebugEnabled())
                log.debug("Failed to load spring component, will not be able to extract userVersion from " +
                    "META-INF/ignite.xml.");
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridComponent> iterator() {
        return comps.iterator();
    }

    /** {@inheritDoc} */
    @Override public List<GridComponent> components() {
        return Collections.unmodifiableList(comps);
    }

    /**
     * @param comp Manager to add.
     */
    public void add(GridComponent comp) {
        add(comp, true);
    }

    /**
     * @param comp Manager to add.
     * @param addToList If {@code true} component is added to components list.
     */
    public void add(GridComponent comp, boolean addToList) {
        assert comp != null;

        /*
         * Managers.
         * ========
         */

        if (comp instanceof GridDeploymentManager)
            depMgr = (GridDeploymentManager)comp;
        else if (comp instanceof GridIoManager)
            ioMgr = (GridIoManager)comp;
        else if (comp instanceof GridDiscoveryManager)
            discoMgr = (GridDiscoveryManager)comp;
        else if (comp instanceof GridCheckpointManager)
            cpMgr = (GridCheckpointManager)comp;
        else if (comp instanceof GridEventStorageManager)
            evtMgr = (GridEventStorageManager)comp;
        else if (comp instanceof GridFailoverManager)
            failoverMgr = (GridFailoverManager)comp;
        else if (comp instanceof GridCollisionManager)
            colMgr = (GridCollisionManager)comp;
        else if (comp instanceof GridSecurityProcessor)
            authProc = (GridSecurityProcessor)comp;
        else if (comp instanceof GridLoadBalancerManager)
            loadMgr = (GridLoadBalancerManager)comp;
        else if (comp instanceof GridSwapSpaceManager)
            swapspaceMgr = (GridSwapSpaceManager)comp;
        else if (comp instanceof GridIndexingManager)
            indexingMgr = (GridIndexingManager)comp;

        /*
         * Processors.
         * ==========
         */

        else if (comp instanceof GridTaskProcessor)
            taskProc = (GridTaskProcessor)comp;
        else if (comp instanceof GridJobProcessor)
            jobProc = (GridJobProcessor)comp;
        else if (comp instanceof GridTimeoutProcessor)
            timeProc = (GridTimeoutProcessor)comp;
        else if (comp instanceof GridClockSyncProcessor)
            clockSyncProc = (GridClockSyncProcessor)comp;
        else if (comp instanceof GridResourceProcessor)
            rsrcProc = (GridResourceProcessor)comp;
        else if (comp instanceof GridJobMetricsProcessor)
            metricsProc = (GridJobMetricsProcessor)comp;
        else if (comp instanceof GridCacheProcessor)
            cacheProc = (GridCacheProcessor)comp;
        else if (comp instanceof GridTaskSessionProcessor)
            sesProc = (GridTaskSessionProcessor)comp;
        else if (comp instanceof GridPortProcessor)
            portProc = (GridPortProcessor)comp;
        else if (comp instanceof GridClosureProcessor)
            closProc = (GridClosureProcessor)comp;
        else if (comp instanceof GridServiceProcessor)
            svcProc = (GridServiceProcessor)comp;
        else if (comp instanceof IgniteScheduleProcessorAdapter)
            scheduleProc = (IgniteScheduleProcessorAdapter)comp;
        else if (comp instanceof GridSegmentationProcessor)
            segProc = (GridSegmentationProcessor)comp;
        else if (comp instanceof GridAffinityProcessor)
            affProc = (GridAffinityProcessor)comp;
        else if (comp instanceof GridRestProcessor)
            restProc = (GridRestProcessor)comp;
        else if (comp instanceof DataStreamProcessor)
            dataLdrProc = (DataStreamProcessor)comp;
        else if (comp instanceof IgfsProcessorAdapter)
            igfsProc = (IgfsProcessorAdapter)comp;
        else if (comp instanceof GridOffHeapProcessor)
            offheapProc = (GridOffHeapProcessor)comp;
        else if (comp instanceof GridContinuousProcessor)
            contProc = (GridContinuousProcessor)comp;
        else if (comp instanceof HadoopProcessorAdapter)
            hadoopProc = (HadoopProcessorAdapter)comp;
        else if (comp instanceof IgniteCacheObjectProcessor)
            cacheObjProc = (IgniteCacheObjectProcessor)comp;
        else if (comp instanceof IgnitePluginProcessor)
            pluginProc = (IgnitePluginProcessor)comp;
        else if (comp instanceof GridQueryProcessor)
            qryProc = (GridQueryProcessor)comp;
        else if (comp instanceof OdbcProcessor)
            odbcProc = (OdbcProcessor)comp;
        else if (comp instanceof DataStructuresProcessor)
            dataStructuresProc = (DataStructuresProcessor)comp;
        else if (comp instanceof ClusterProcessor)
            cluster = (ClusterProcessor)comp;
        else if (comp instanceof PlatformProcessor)
            platformProc = (PlatformProcessor)comp;
        else if (comp instanceof PoolProcessor)
            poolProc = (PoolProcessor) comp;
        else if (!(comp instanceof DiscoveryNodeValidationProcessor))
            assert (comp instanceof GridPluginComponent) : "Unknown manager class: " + comp.getClass();

        if (addToList)
            comps.add(comp);
    }

    /**
     * @param helper Helper to add.
     */
    public void addHelper(Object helper) {
        assert helper != null;

        if (helper instanceof IgfsHelper)
            igfsHelper = (IgfsHelper)helper;
        else if (helper instanceof HadoopHelper)
            hadoopHelper = (HadoopHelper)helper;
        else
            assert false : "Unknown helper class: " + helper.getClass();
    }

    /** {@inheritDoc} */
    @Override public boolean isStopping() {
        return ((IgniteKernal)grid).isStopping();
    }

    /** {@inheritDoc} */
    @Override public UUID localNodeId() {
        if (locNode != null)
            return locNode.id();

        if (discoMgr != null)
            locNode = discoMgr.localNode();

        return locNode != null ? locNode.id() : config().getNodeId();
    }

    /** {@inheritDoc} */
    @Override public String gridName() {
        return cfg.getGridName();
    }

    /** {@inheritDoc} */
    @Override public GridKernalGateway gateway() {
        return gw;
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return grid;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration config() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public GridTaskProcessor task() {
        return taskProc;
    }

    /** {@inheritDoc} */
    @Override public GridJobProcessor job() {
        return jobProc;
    }

    /** {@inheritDoc} */
    @Override public GridTimeoutProcessor timeout() {
        return timeProc;
    }

    /** {@inheritDoc} */
    @Override public GridClockSyncProcessor clockSync() {
        return clockSyncProc;
    }

    /** {@inheritDoc} */
    @Override public GridResourceProcessor resource() {
        return rsrcProc;
    }

    /** {@inheritDoc} */
    @Override public GridJobMetricsProcessor jobMetric() {
        return metricsProc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProcessor cache() {
        return cacheProc;
    }

    /** {@inheritDoc} */
    @Override public GridTaskSessionProcessor session() {
        return sesProc;
    }

    /** {@inheritDoc} */
    @Override public GridClosureProcessor closure() {
        return closProc;
    }

    /** {@inheritDoc} */
    @Override public GridServiceProcessor service() {
        return svcProc;
    }

    /** {@inheritDoc} */
    @Override public GridPortProcessor ports() {
        return portProc;
    }

    /** {@inheritDoc} */
    @Override public GridOffHeapProcessor offheap() {
        return offheapProc;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduleProcessorAdapter schedule() {
        return scheduleProc;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentManager deploy() {
        return depMgr;
    }

    /** {@inheritDoc} */
    @Override public GridIoManager io() {
        return ioMgr;
    }

    /** {@inheritDoc} */
    @Override public GridDiscoveryManager discovery() {
        return discoMgr;
    }

    /** {@inheritDoc} */
    @Override public GridCheckpointManager checkpoint() {
        return cpMgr;
    }

    /** {@inheritDoc} */
    @Override public GridEventStorageManager event() {
        return evtMgr;
    }

    /** {@inheritDoc} */
    @Override public GridFailoverManager failover() {
        return failoverMgr;
    }

    /** {@inheritDoc} */
    @Override public GridCollisionManager collision() {
        return colMgr;
    }

    /** {@inheritDoc} */
    @Override public GridSecurityProcessor security() {
        return authProc;
    }

    /** {@inheritDoc} */
    @Override public GridLoadBalancerManager loadBalancing() {
        return loadMgr;
    }

    /** {@inheritDoc} */
    @Override public GridSwapSpaceManager swap() {
        return swapspaceMgr;
    }

    /** {@inheritDoc} */
    @Override public GridIndexingManager indexing() {
        return indexingMgr;
    }

    /** {@inheritDoc} */
    @Override public GridAffinityProcessor affinity() {
        return affProc;
    }

    /** {@inheritDoc} */
    @Override public GridRestProcessor rest() {
        return restProc;
    }

    /** {@inheritDoc} */
    @Override public GridSegmentationProcessor segmentation() {
        return segProc;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> DataStreamProcessor<K, V> dataStream() {
        return (DataStreamProcessor<K, V>)dataLdrProc;
    }

    /** {@inheritDoc} */
    @Override public IgfsProcessorAdapter igfs() {
        return igfsProc;
    }

    /** {@inheritDoc} */
    @Override public IgfsHelper igfsHelper() {
        return igfsHelper;
    }

    /** {@inheritDoc} */
    @Override public HadoopHelper hadoopHelper() {
        return hadoopHelper;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousProcessor continuous() {
        return contProc;
    }

    /** {@inheritDoc} */
    @Override public HadoopProcessorAdapter hadoop() {
        return hadoopProc;
    }

    /** {@inheritDoc} */
    @Override public PoolProcessor pools() {
        return poolProc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService utilityCachePool() {
        return utilityCachePool;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService marshallerCachePool() {
        return marshCachePool;
    }

    /** {@inheritDoc} */
    @Override public IgniteStripedThreadPoolExecutor asyncCallbackPool() {
        return callbackExecSvc;
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheObjectProcessor cacheObjects() {
        return cacheObjProc;
    }

    /** {@inheritDoc} */
    @Override public GridQueryProcessor query() {
        return qryProc;
    }

    /** {@inheritDoc} */
    @Override public OdbcProcessor odbc() {
        return odbcProc;
    }

    /** {@inheritDoc} */
    @Override public DataStructuresProcessor dataStructures() {
        return dataStructuresProc;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(String ctgr) {
        return config().getGridLogger().getLogger(ctgr);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return log(cls.getName());
    }

    /** {@inheritDoc} */
    @Override public void markSegmented() {
        segFlag = true;
    }

    /** {@inheritDoc} */
    @Override public boolean segmented() {
        return segFlag;
    }

    /** {@inheritDoc} */
    @Override public GridClockSource timeSource() {
        return clockSrc;
    }

    /**
     * Sets time source. For test purposes only.
     *
     * @param clockSrc Time source.
     */
    public void timeSource(GridClockSource clockSrc) {
        this.clockSrc = clockSrc;
    }

    /** {@inheritDoc} */
    @Override public GridPerformanceSuggestions performance() {
        return perf;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Grid memory stats [grid=" + gridName() + ']');

        for (GridComponent comp : comps)
            comp.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return config().isDaemon() || "true".equalsIgnoreCase(System.getProperty(IGNITE_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public String userVersion(ClassLoader ldr) {
        return spring != null ? spring.userVersion(ldr, log(spring.getClass())) : U.DFLT_USER_VERSION;
    }

    /** {@inheritDoc} */
    @Override public PluginProvider pluginProvider(String name) throws PluginNotFoundException {
        PluginProvider plugin = pluginProc.pluginProvider(name);

        if (plugin == null)
            throw new PluginNotFoundException(name);

        return plugin;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T createComponent(Class<T> cls) {
        T res = pluginProc.createComponent(cls);

        if (res != null)
            return res;

        if (cls.equals(IgniteCacheObjectProcessor.class))
            return (T)new CacheObjectBinaryProcessorImpl(this);

        if (cls.equals(CacheConflictResolutionManager.class))
            return null;

        throw new IgniteException("Unsupported component type: " + cls);
    }

    /**
     * @return Plugin manager.
     */
    @Override public IgnitePluginProcessor plugins() {
        return pluginProc;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, grid.name());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        U.readString(in); // Read for compatibility only. See #readResolve().
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            return IgnitionEx.localIgnite().context();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getExecutorService() {
        return execSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getSystemExecutorService() {
        return sysExecSvc;
    }

    /** {@inheritDoc} */
    @Override public StripedExecutor getStripedExecutorService() {
        return stripedExecSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getManagementExecutorService() {
        return mgmtExecSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getPeerClassLoadingExecutorService() {
        return p2pExecSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getIgfsExecutorService() {
        return igfsExecSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getRestExecutorService() {
        return restExecSvc;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getAffinityExecutorService() {
        return affExecSvc;
    }

    /** {@inheritDoc} */
    @Override @Nullable public ExecutorService getIndexingExecutorService() {
        return idxExecSvc;
    }

    /** {@inheritDoc} */
    @Override public IgniteExceptionRegistry exceptionRegistry() {
        return IgniteExceptionRegistry.get();
    }

    /** {@inheritDoc} */
    @Override public Object nodeAttribute(String key) {
        return attrs.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNodeAttribute(String key) {
        return attrs.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public Object addNodeAttribute(String key, Object val) {
        return attrs.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> nodeAttributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public ClusterProcessor cluster() {
        return cluster;
    }

    /** {@inheritDoc} */
    @Override public MarshallerContextImpl marshallerContext() {
        return marshCtx;
    }

    /** {@inheritDoc} */
    @Override public boolean clientNode() {
        return cfg.isClientMode() || cfg.isDaemon();
    }

    /** {@inheritDoc} */
    @Override public boolean clientDisconnected() {
        if (locNode == null)
            locNode = discoMgr != null ? discoMgr.localNode() : null;

        return locNode != null ? (locNode.isClient() && disconnected) : false;
    }

    /** {@inheritDoc} */
    @Override public PlatformProcessor platform() {
        return platformProc;
    }

    /**
     * @param disconnected Disconnected flag.
     */
    void disconnected(boolean disconnected) {
        this.disconnected = disconnected;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKernalContextImpl.class, this);
    }
}
