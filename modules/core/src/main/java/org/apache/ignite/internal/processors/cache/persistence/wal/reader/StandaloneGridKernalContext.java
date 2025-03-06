/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.transform.CacheObjectTransformerProcessor;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManager;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.failover.GridFailoverManager;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.JmxSystemViewExporterSpi;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.IgniteDefragmentation;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessor;
import org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.rest.IgniteRestProcessor;
import org.apache.ignite.internal.processors.schedule.IgniteScheduleProcessorAdapter;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.internal.processors.service.IgniteServiceProcessor;
import org.apache.ignite.internal.processors.session.GridTaskSessionProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy context for offline utilities. All grid components registered in the standalone context
 * must be properly stopped since the lifecycle of them are controlled by kernal.
 *
 * @see org.apache.ignite.internal.GridComponent#stop(boolean)
 */
public class StandaloneGridKernalContext implements GridKernalContext {
    /** Config for fake Ignite instance. */
    private final IgniteConfiguration cfg;

    /** List of registered components. */
    private final List<GridComponent> comps = new LinkedList<>();

    /** Logger. */
    private IgniteLogger log;

    /** Node file tree. */
    private NodeFileTree ft;

    /** Empty plugin processor. */
    private IgnitePluginProcessor pluginProc;

    /** */
    private GridResourceProcessor rsrcProc;

    /** Metrics manager. */
    private final GridMetricManager metricMgr;

    /** System view manager. */
    private final GridSystemViewManager sysViewMgr;

    /** Timeout processor. */
    private final GridTimeoutProcessor timeoutProc;

    /** */
    @GridToStringExclude
    private CacheObjectTransformerProcessor transProc;

    /**
     * Cache object processor. Used for converting cache objects and keys into binary objects. Null means there is no
     * convert is configured. All entries in this case will be lazy data entries.
     */
    @Nullable private IgniteCacheObjectProcessor cacheObjProcessor;

    /** Marshaller context implementation. */
    private MarshallerContextImpl marshallerCtx;

    /** */
    @Nullable private CompressionProcessor compressProc;

    /**
     * @param log Logger.
     * @param ft Node file tree.
     */
    public StandaloneGridKernalContext(
        IgniteLogger log,
        @Nullable NodeFileTree ft
    ) throws IgniteCheckedException {
        this(log, null, ft);
    }

    /**
     * @param log Logger.
     * @param compressProc Compression processor.
     * @param ft Node file tree {@code null} means no specific tree is configured. <br>
     */
    public StandaloneGridKernalContext(
        IgniteLogger log,
        @Nullable CompressionProcessor compressProc,
        @Nullable NodeFileTree ft
    ) throws IgniteCheckedException {
        this.log = log;
        this.ft = ft;

        marshallerCtx = new MarshallerContextImpl(null, MarshallerUtils.classNameFilter(getClass().getClassLoader()));
        cfg = prepareIgniteConfiguration();

        try {
            pluginProc = new StandaloneIgnitePluginProcessor(this, cfg);
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Must not fail on empty providers list.", e);
        }

        rsrcProc = new GridResourceProcessor(this);
        metricMgr = new GridMetricManager(this);
        sysViewMgr = new GridSystemViewManager(this);
        timeoutProc = new GridTimeoutProcessor(this);
        transProc = createComponent(CacheObjectTransformerProcessor.class);

        // Fake folder provided to perform processor startup on empty folder.
        cacheObjProcessor = binaryProcessor(this, ft != null
            ? ft.binaryMeta()
            : new SharedFileTree(new File(".")).binaryMetaRoot().getAbsoluteFile());

        comps.add(rsrcProc);
        comps.add(cacheObjProcessor);
        comps.add(metricMgr);
        comps.add(timeoutProc);

        if (ft != null) {
            marshallerCtx.setMarshallerMappingFileStoreDir(ft.marshaller());
            marshallerCtx.onMarshallerProcessorStarted(this, null);
        }

        this.compressProc = compressProc;
    }

    /**
     * Creates binary processor which allows to convert WAL records into objects
     *
     * @param ctx kernal context
     * @param binaryMetadataFileStoreDir folder specifying location of metadata File Store
     *
     * {@code null} means no specific folder is configured. <br> In this case folder for metadata is composed from work
     * directory and consistentId
     * @return Cache object processor able to restore data records content into binary objects
     */
    private IgniteCacheObjectProcessor binaryProcessor(
        final GridKernalContext ctx,
        final File binaryMetadataFileStoreDir) {

        final CacheObjectBinaryProcessorImpl proc = new CacheObjectBinaryProcessorImpl(ctx);
        proc.setBinaryMetadataFileStoreDir(binaryMetadataFileStoreDir);

        return proc;
    }

    /**
     * @return Ignite configuration which allows to start requied processors for WAL reader
     */
    protected IgniteConfiguration prepareIgniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(new StandaloneNoopDiscoverySpi());
        cfg.setCommunicationSpi(new StandaloneNoopCommunicationSpi());

        final Marshaller marshaller = new BinaryMarshaller();
        cfg.setMarshaller(marshaller);

        final DataStorageConfiguration pstCfg = new DataStorageConfiguration();
        final DataRegionConfiguration regCfg = new DataRegionConfiguration();
        regCfg.setPersistenceEnabled(true);
        pstCfg.setDefaultDataRegionConfiguration(regCfg);

        cfg.setDataStorageConfiguration(pstCfg);

        marshaller.setContext(marshallerCtx);

        cfg.setMetricExporterSpi(new NoopMetricExporterSpi());
        cfg.setSystemViewExporterSpi(new JmxSystemViewExporterSpi());
        cfg.setGridLogger(log);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public List<GridComponent> components() {
        return Collections.unmodifiableList(comps);
    }

    /** {@inheritDoc} */
    @Override public UUID localNodeId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String igniteInstanceName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(String ctgr) {
        return log;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return log;
    }

    /** {@inheritDoc} */
    @Override public boolean isStopping() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridKernalGateway gateway() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        final IgniteEx kernal = new IgniteKernal() {
            /**
             * Override to return the non-null context instance to make metric SPIs happy.<br>
             *
             * Say the SqlViewMetricExporterSpi one which may be automatically added by
             * the {@link GridMetricManager} if indexing or query engine are found in classpath
             * (which is the default behaviour).
             *
             * @return Kernal context.
             */
            @Override public GridKernalContext context() {
                return StandaloneGridKernalContext.this;
            }
        };
        try {
            setField(kernal, "cfg", cfg);
            setField(kernal, "igniteInstanceName", cfg.getIgniteInstanceName());
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            log.error("", e);
        }
        return kernal;
    }

    /** */
    private void setField(IgniteEx kernal, String name, Object val) throws NoSuchFieldException, IllegalAccessException {
        Field field = kernal.getClass().getSuperclass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(kernal, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration config() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public GridTaskProcessor task() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridAffinityProcessor affinity() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridJobProcessor job() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridTimeoutProcessor timeout() {
        return timeoutProc;
    }

    /** {@inheritDoc} */
    @Override public GridResourceProcessor resource() {
        return rsrcProc;
    }

    /** {@inheritDoc} */
    @Override public GridJobMetricsProcessor jobMetric() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridMetricManager metric() {
        return metricMgr;
    }

    /** {@inheritDoc} */
    @Override public GridSystemViewManager systemView() {
        return sysViewMgr;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProcessor cache() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridClusterStateProcessor state() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorage distributedMetastorage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public DistributedConfigurationProcessor distributedConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tracing tracing() {
        return new NoopTracing();
    }

    /** {@inheritDoc} */
    @Override public GridTaskSessionProcessor session() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridClosureProcessor closure() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServiceProcessor service() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridPortProcessor ports() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduleProcessorAdapter schedule() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public MaintenanceRegistry maintenanceRegistry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObjectTransformerProcessor transformer() {
        return transProc;
    }

    /** {@inheritDoc} */
    @Override public IgniteRestProcessor rest() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridSegmentationProcessor segmentation() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> DataStreamProcessor<K, V> dataStream() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousProcessor continuous() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PoolProcessor pools() {
        return new PoolProcessor(this);
    }

    /** {@inheritDoc} */
    @Override public GridMarshallerMappingProcessor mapping() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheObjectProcessor cacheObjects() {
        return cacheObjProcessor;
    }

    /** {@inheritDoc} */
    @Override public GridQueryProcessor query() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProcessor clientListener() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePluginProcessor plugins() {
        return pluginProc;
    }

    /** {@inheritDoc} */
    @Override public GridDeploymentManager deploy() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridIoManager io() {
        return new GridIoManager(this);
    }

    /** {@inheritDoc} */
    @Override public GridDiscoveryManager discovery() {
        return new GridDiscoveryManager(this);
    }

    /** {@inheritDoc} */
    @Override public GridCheckpointManager checkpoint() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridEventStorageManager event() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridFailoverManager failover() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCollisionManager collision() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteSecurity security() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridLoadBalancerManager loadBalancing() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridIndexingManager indexing() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IndexProcessor indexProcessor() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridEncryptionManager encryption() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteDefragmentation defragmentation() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public WorkersRegistry workersRegistry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public DataStructuresProcessor dataStructures() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean invalid() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean segmented() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public FailureProcessor failure() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
    }

    /** {@inheritDoc} */
    @Override public GridPerformanceSuggestions performance() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String userVersion(ClassLoader ldr) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PluginProvider pluginProvider(String name) throws PluginNotFoundException {
        PluginProvider plugin = pluginProc.pluginProvider(name);

        if (plugin == null)
            throw new PluginNotFoundException(name);

        return plugin;
    }

    /** {@inheritDoc} */
    @Override public <T> T createComponent(Class<T> cls) {
        T res = pluginProc.createComponent(cls);

        if (res != null)
            return res;

        if (cls.equals(CacheObjectTransformerProcessor.class))
            return null;

        throw new IgniteException("Unsupported component type: " + cls);
    }

    /** {@inheritDoc} */
    @Override public IgniteExceptionRegistry exceptionRegistry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object nodeAttribute(String key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNodeAttribute(String key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object addNodeAttribute(String key, Object val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> nodeAttributes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ClusterProcessor cluster() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public MarshallerContextImpl marshallerContext() {
        return marshallerCtx;
    }

    /** {@inheritDoc} */
    @Override public boolean clientNode() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean clientDisconnected() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public PlatformProcessor platform() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridInternalSubscriptionProcessor internalSubscriptionProcessor() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean recoveryMode() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public PdsFoldersResolver pdsFolderResolver() {
        return new PdsFoldersResolver() {
            /** {@inheritDoc} */
            @Override public PdsFolderSettings resolveFolders() {
                return ft != null
                    ? new PdsFolderSettings<>(ft.root(), ft.folderName())
                    : new PdsFolderSettings<>(new File("."), U.maskForFileName(""));
            }

            /** {@inheritDoc} */
            @Override public NodeFileTree fileTree() {
                return ft != null ? ft : new NodeFileTree(cfg, resolveFolders().folderName());
            }
        };
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<GridComponent> iterator() {
        return comps.iterator();
    }

    /** {@inheritDoc} */
    @Override public CompressionProcessor compress() {
        return compressProc;
    }

    /** {@inheritDoc} */
    @Override public LongJVMPauseDetector longJvmPauseDetector() {
        return new LongJVMPauseDetector(log);
    }

    /** {@inheritDoc} */
    @Override public DiagnosticProcessor diagnostic() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public DurableBackgroundTasksProcessor durableBackgroundTask() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PerformanceStatisticsProcessor performanceStatistics() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Executor getAsyncContinuationExecutor() {
        return null;
    }

    /**
     * @param kctx Kernal context.
     * @throws IgniteCheckedException In case of any error.
     */
    public static void startAllComponents(GridKernalContext kctx) throws IgniteCheckedException {
        for (GridComponent comp : kctx)
            comp.start();
    }

    /**
     * @param kctx Kernal context.
     * @throws IgniteCheckedException In case of any error.
     */
    public static void closeAllComponents(GridKernalContext kctx) throws IgniteCheckedException {
        for (GridComponent comp : kctx)
            comp.stop(true);
    }
}
