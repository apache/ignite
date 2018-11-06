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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManager;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.failover.GridFailoverManager;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerManager;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.processors.hadoop.HadoopProcessorAdapter;
import org.apache.ignite.internal.processors.igfs.IgfsHelper;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
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
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy grid kernal context
 */
public class StandaloneGridKernalContext implements GridKernalContext {
    /** Binary metadata file store folder. */
    public static final String BINARY_META_FOLDER = "binary_meta";

    /** Config for fake Ignite instance. */
    private final IgniteConfiguration cfg;

    /** Logger. */
    private IgniteLogger log;

    /** Empty plugin processor. */
    private IgnitePluginProcessor pluginProc;

    /**
     * Cache object processor. Used for converting cache objects and keys into binary objects. Null means there is no
     * convert is configured. All entries in this case will be lazy data entries.
     */
    @Nullable private IgniteCacheObjectProcessor cacheObjProcessor;

    /** Marshaller context implementation. */
    private MarshallerContextImpl marshallerCtx;

    /**
     * @param log Logger.
     * @param binaryMetadataFileStoreDir folder specifying location of metadata File Store.
     * {@code null} means no specific folder is configured. <br>
     *
     * @param marshallerMappingFileStoreDir folder specifying location of marshaller mapping file store.
     * {@code null} means no specific folder is configured.
     * Providing {@code null} will disable unmarshall for non primitive objects, BinaryObjects will be provided <br>
     */
    public StandaloneGridKernalContext(
        IgniteLogger log,
        @Nullable File binaryMetadataFileStoreDir,
        @Nullable File marshallerMappingFileStoreDir
    ) throws IgniteCheckedException {
        this.log = log;

        try {
            pluginProc = new StandaloneIgnitePluginProcessor(this, config());
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Must not fail on empty providers list.", e);
        }

        this.marshallerCtx = new MarshallerContextImpl(null, null);
        this.cfg = prepareIgniteConfiguration();

        // Fake folder provided to perform processor startup on empty folder.
        if (binaryMetadataFileStoreDir == null)
            binaryMetadataFileStoreDir = new File(BINARY_META_FOLDER).getAbsoluteFile();

        this.cacheObjProcessor = binaryProcessor(this, binaryMetadataFileStoreDir);

        if (marshallerMappingFileStoreDir != null) {
            marshallerCtx.setMarshallerMappingFileStoreDir(marshallerMappingFileStoreDir);
            marshallerCtx.onMarshallerProcessorStarted(this, null);
        }
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
     * @throws IgniteCheckedException Throws in case of initialization errors.
     */
    private IgniteCacheObjectProcessor binaryProcessor(
        final GridKernalContext ctx,
        final File binaryMetadataFileStoreDir) throws IgniteCheckedException {

        final CacheObjectBinaryProcessorImpl processor = new CacheObjectBinaryProcessorImpl(ctx);
        processor.setBinaryMetadataFileStoreDir(binaryMetadataFileStoreDir);
        processor.start();
        return processor;
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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public List<GridComponent> components() {
        return null;
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
        final IgniteEx kernal = new IgniteKernal();
        try {
            Field fieldCfg = kernal.getClass().getDeclaredField("cfg");
            fieldCfg.setAccessible(true);
            fieldCfg.set(kernal, cfg);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            log.error("", e);
        }
        return kernal;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridResourceProcessor resource() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridJobMetricsProcessor jobMetric() {
        return null;
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
    @Override public GridTaskSessionProcessor session() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridClosureProcessor closure() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridServiceProcessor service() {
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
    @Override public GridRestProcessor rest() {
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
    @Override public IgniteAuthenticationProcessor authentication() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsProcessorAdapter igfs() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsHelper igfsHelper() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousProcessor continuous() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public HadoopProcessorAdapter hadoop() {
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
    @Override public HadoopHelper hadoopHelper() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService utilityCachePool() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteStripedThreadPoolExecutor asyncCallbackPool() {
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
    @Override public ClientListenerProcessor sqlListener() {
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
    @Override public GridSecurityProcessor security() {
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
    @Override public boolean isDaemon() {
        return false;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> T createComponent(Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getServiceExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getSystemExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public StripedExecutor getStripedExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getManagementExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getPeerClassLoadingExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getIgfsExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public StripedExecutor getDataStreamerExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getRestExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getAffinityExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ExecutorService getIndexingExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getQueryExecutorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<String, ? extends ExecutorService> customExecutors() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService getSchemaExecutorService() {
        return null;
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
    @Override public PdsFoldersResolver pdsFolderResolver() {
        return new PdsFoldersResolver() {
            /** {@inheritDoc} */
            @Override public PdsFolderSettings resolveFolders() {
                return new PdsFolderSettings(new File("."), U.maskForFileName(""));
            }
        };
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<GridComponent> iterator() {
        return null;
    }
}
