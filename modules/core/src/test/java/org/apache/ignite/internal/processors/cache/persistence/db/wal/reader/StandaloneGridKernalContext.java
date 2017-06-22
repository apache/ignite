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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.MarshallerContextImpl;
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
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.processors.hadoop.HadoopProcessorAdapter;
import org.apache.ignite.internal.processors.igfs.IgfsHelper;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor;
import org.apache.ignite.internal.processors.odbc.SqlListenerProcessor;
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
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy grid kernal context
 */
public class StandaloneGridKernalContext implements GridKernalContext {
    private IgniteLogger log;

    public StandaloneGridKernalContext(IgniteLogger log) {
        this.log = log;
    }

    @Override public List<GridComponent> components() {
        return null;
    }

    @Override public UUID localNodeId() {
        return null;
    }

    @Override public String igniteInstanceName() {
        return null;
    }

    @Override public IgniteLogger log(String ctgr) {
        return log;
    }

    @Override public IgniteLogger log(Class<?> cls) {
        return log;
    }

    @Override public boolean isStopping() {
        return false;
    }

    @Override public GridKernalGateway gateway() {
        return null;
    }

    @Override public IgniteEx grid() {
        return null;
    }

    @Override public IgniteConfiguration config() {
        return null;
    }

    @Override public GridTaskProcessor task() {
        return null;
    }

    @Override public GridAffinityProcessor affinity() {
        return null;
    }

    @Override public GridJobProcessor job() {
        return null;
    }

    @Override public GridTimeoutProcessor timeout() {
        return null;
    }

    @Override public GridResourceProcessor resource() {
        return null;
    }

    @Override public GridJobMetricsProcessor jobMetric() {
        return null;
    }

    @Override public GridCacheProcessor cache() {
        return null;
    }

    @Override public GridClusterStateProcessor state() {
        return null;
    }

    @Override public GridTaskSessionProcessor session() {
        return null;
    }

    @Override public GridClosureProcessor closure() {
        return null;
    }

    @Override public GridServiceProcessor service() {
        return null;
    }

    @Override public GridPortProcessor ports() {
        return null;
    }

    @Override public IgniteScheduleProcessorAdapter schedule() {
        return null;
    }

    @Override public GridRestProcessor rest() {
        return null;
    }

    @Override public GridSegmentationProcessor segmentation() {
        return null;
    }

    @Override public <K, V> DataStreamProcessor<K, V> dataStream() {
        return null;
    }

    @Override public IgfsProcessorAdapter igfs() {
        return null;
    }

    @Override public IgfsHelper igfsHelper() {
        return null;
    }

    @Override public GridContinuousProcessor continuous() {
        return null;
    }

    @Override public HadoopProcessorAdapter hadoop() {
        return null;
    }

    @Override public PoolProcessor pools() {
        return null;
    }

    @Override public GridMarshallerMappingProcessor mapping() {
        return null;
    }

    @Override public HadoopHelper hadoopHelper() {
        return null;
    }

    @Override public ExecutorService utilityCachePool() {
        return null;
    }

    @Override public IgniteStripedThreadPoolExecutor asyncCallbackPool() {
        return null;
    }

    @Override public IgniteCacheObjectProcessor cacheObjects() {
        return null;
    }

    @Override public GridQueryProcessor query() {
        return null;
    }

    @Override public SqlListenerProcessor sqlListener() {
        return null;
    }

    @Override public IgnitePluginProcessor plugins() {
        return null;
    }

    @Override public GridDeploymentManager deploy() {
        return null;
    }

    @Override public GridIoManager io() {
        return null;
    }

    @Override public GridDiscoveryManager discovery() {
        return null;
    }

    @Override public GridCheckpointManager checkpoint() {
        return null;
    }

    @Override public GridEventStorageManager event() {
        return null;
    }

    @Override public GridFailoverManager failover() {
        return null;
    }

    @Override public GridCollisionManager collision() {
        return null;
    }

    @Override public GridSecurityProcessor security() {
        return null;
    }

    @Override public GridLoadBalancerManager loadBalancing() {
        return null;
    }

    @Override public GridIndexingManager indexing() {
        return null;
    }

    @Override public DataStructuresProcessor dataStructures() {
        return null;
    }

    @Override public void markSegmented() {

    }

    @Override public boolean segmented() {
        return false;
    }

    @Override public void printMemoryStats() {

    }

    @Override public boolean isDaemon() {
        return false;
    }

    @Override public GridPerformanceSuggestions performance() {
        return null;
    }

    @Override public String userVersion(ClassLoader ldr) {
        return null;
    }

    @Override public PluginProvider pluginProvider(String name) throws PluginNotFoundException {
        return null;
    }

    @Override public <T> T createComponent(Class<T> cls) {
        return null;
    }

    @Override public ExecutorService getExecutorService() {
        return null;
    }

    @Override public ExecutorService getServiceExecutorService() {
        return null;
    }

    @Override public ExecutorService getSystemExecutorService() {
        return null;
    }

    @Override public StripedExecutor getStripedExecutorService() {
        return null;
    }

    @Override public ExecutorService getManagementExecutorService() {
        return null;
    }

    @Override public ExecutorService getPeerClassLoadingExecutorService() {
        return null;
    }

    @Override public ExecutorService getIgfsExecutorService() {
        return null;
    }

    @Override public ExecutorService getDataStreamerExecutorService() {
        return null;
    }

    @Override public ExecutorService getRestExecutorService() {
        return null;
    }

    @Override public ExecutorService getAffinityExecutorService() {
        return null;
    }

    @Nullable @Override public ExecutorService getIndexingExecutorService() {
        return null;
    }

    @Override public ExecutorService getQueryExecutorService() {
        return null;
    }

    @Nullable @Override public Map<String, ? extends ExecutorService> customExecutors() {
        return null;
    }

    @Override public ExecutorService getSchemaExecutorService() {
        return null;
    }

    @Override public IgniteExceptionRegistry exceptionRegistry() {
        return null;
    }

    @Override public Object nodeAttribute(String key) {
        return null;
    }

    @Override public boolean hasNodeAttribute(String key) {
        return false;
    }

    @Override public Object addNodeAttribute(String key, Object val) {
        return null;
    }

    @Override public Map<String, Object> nodeAttributes() {
        return null;
    }

    @Override public ClusterProcessor cluster() {
        return null;
    }

    @Override public MarshallerContextImpl marshallerContext() {
        return null;
    }

    @Override public boolean clientNode() {
        return false;
    }

    @Override public boolean clientDisconnected() {
        return false;
    }

    @Override public PlatformProcessor platform() {
        return null;
    }

    @NotNull @Override public Iterator<GridComponent> iterator() {
        return null;
    }
}
