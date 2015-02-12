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
import org.apache.ignite.configuration.*;
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
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.clock.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.processors.dataload.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.processors.email.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.job.*;
import org.apache.ignite.internal.processors.jobmetrics.*;
import org.apache.ignite.internal.processors.offheap.*;
import org.apache.ignite.internal.processors.plugin.*;
import org.apache.ignite.internal.processors.port.*;
import org.apache.ignite.internal.processors.portable.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.schedule.*;
import org.apache.ignite.internal.processors.segmentation.*;
import org.apache.ignite.internal.processors.service.*;
import org.apache.ignite.internal.processors.session.*;
import org.apache.ignite.internal.processors.streamer.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.plugin.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridToStringExclude
public interface GridKernalContext extends Iterable<GridComponent> {
    /**
     * Gets list of all grid components in the order they were added.
     *
     * @return List of all grid components in the order they were added.
     */
    public List<GridComponent> components();

    /**
     * Gets local node ID.
     *
     * @return Local node ID.
     */
    public UUID localNodeId();

    /**
     * Gets grid name.
     *
     * @return Grid name.
     */
    public String gridName();

    /**
     * Gets logger.
     *
     * @return Logger.
     */
    public IgniteLogger log();

    /**
     * Gets logger for given class.
     *
     * @param cls Class to get logger for.
     * @return Logger.
     */
    public IgniteLogger log(Class<?> cls);

    /**
     * @return {@code True} if grid is in the process of stopping.
     */
    public boolean isStopping();

    /**
     * Gets kernal gateway.
     *
     * @return Kernal gateway.
     */
    public GridKernalGateway gateway();

    /**
     * Gets grid instance managed by kernal.
     *
     * @return Grid instance.
     */
    public IgniteEx grid();

    /**
     * Gets grid configuration.
     *
     * @return Grid configuration.
     */
    public IgniteConfiguration config();

    /**
     * Gets task processor.
     *
     * @return Task processor.
     */
    public GridTaskProcessor task();

    /**
     * Gets cache data affinity processor.
     *
     * @return Cache data affinity processor.
     */
    public GridAffinityProcessor affinity();

    /**
     * Gets job processor.
     *
     * @return Job processor
     */
    public GridJobProcessor job();

    /**
     * Gets offheap processor.
     *
     * @return Off-heap processor.
     */
    public GridOffHeapProcessor offheap();

    /**
     * Gets timeout processor.
     *
     * @return Timeout processor.
     */
    public GridTimeoutProcessor timeout();

    /**
     * Gets time processor.
     *
     * @return Time processor.
     */
    public GridClockSyncProcessor clockSync();

    /**
     * Gets resource processor.
     *
     * @return Resource processor.
     */
    public GridResourceProcessor resource();

    /**
     * Gets job metric processor.
     *
     * @return Metrics processor.
     */
    public GridJobMetricsProcessor jobMetric();

    /**
     * Gets caches processor.
     *
     * @return Cache processor.
     */
    public GridCacheProcessor cache();

    /**
     * Gets task session processor.
     *
     * @return Session processor.
     */
    public GridTaskSessionProcessor session();

    /**
     * Gets closure processor.
     *
     * @return Closure processor.
     */
    public GridClosureProcessor closure();

    /**
     * Gets service processor.
     *
     * @return Service processor.
     */
    public GridServiceProcessor service();

    /**
     * Gets port processor.
     *
     * @return Port processor.
     */
    public GridPortProcessor ports();

    /**
     * Gets email processor.
     *
     * @return Email processor.
     */
    public IgniteEmailProcessorAdapter email();

    /**
     * Gets schedule processor.
     *
     * @return Schedule processor.
     */
    public IgniteScheduleProcessorAdapter schedule();

    /**
     * Gets REST processor.
     *
     * @return REST processor.
     */
    public GridRestProcessor rest();

    /**
     * Gets segmentation processor.
     *
     * @return Segmentation processor.
     */
    public GridSegmentationProcessor segmentation();

    /**
     * Gets data loader processor.
     *
     * @return Data loader processor.
     */
    public <K, V> GridDataLoaderProcessor<K, V> dataLoad();

    /**
     * Gets file system processor.
     *
     * @return File system processor.
     */
    public IgniteFsProcessorAdapter ggfs();

    /**
     * Gets GGFS utils processor.
     *
     * @return GGFS utils processor.
     */
    public IgniteFsHelper ggfsHelper();

    /**
     * Gets stream processor.
     *
     * @return Stream processor.
     */
    public GridStreamProcessor stream();

    /**
     * Gets event continuous processor.
     *
     * @return Event continuous processor.
     */
    public GridContinuousProcessor continuous();

    /**
     * Gets Hadoop processor.
     *
     * @return Hadoop processor.
     */
    public IgniteHadoopProcessorAdapter hadoop();

    /**
     * Gets utility cache pool.
     *
     * @return DR pool.
     */
    public ExecutorService utilityCachePool();

    /**
     * Gets portable processor.
     *
     * @return Portable processor.
     */
    public GridPortableProcessor portable();

    /**
     * Gets query processor.
     *
     * @return Query processor.
     */
    public GridQueryProcessor query();

    /**
     * @return Plugin processor.
     */
    public IgnitePluginProcessor plugins();

    /**
     * Gets deployment manager.
     *
     * @return Deployment manager.
     */
    public GridDeploymentManager deploy();

    /**
     * Gets communication manager.
     *
     * @return Communication manager.
     */
    public GridIoManager io();

    /**
     * Gets discovery manager.
     *
     * @return Discovery manager.
     */
    public GridDiscoveryManager discovery();

    /**
     * Gets checkpoint manager.
     *
     * @return Checkpoint manager.
     */
    public GridCheckpointManager checkpoint();

    /**
     * Gets event storage manager.
     *
     * @return Event storage manager.
     */
    public GridEventStorageManager event();

    /**
     * Gets failover manager.
     *
     * @return Failover manager.
     */
    public GridFailoverManager failover();

    /**
     * Gets collision manager.
     *
     * @return Collision manager.
     */
    public GridCollisionManager collision();

    /**
     * Gets authentication manager.
     *
     * @return Authentication manager.
     */
    public GridSecurityManager security();

    /**
     * Gets secure session manager.
     *
     * @return Secure session manager.
     */
    public GridSecureSessionManager secureSession();

    /**
     * Gets load balancing manager.
     *
     * @return Load balancing manager.
     */
    public GridLoadBalancerManager loadBalancing();

    /**
     * Gets swap space manager.
     *
     * @return Swap space manager.
     */
    public GridSwapSpaceManager swap();

    /**
     * Gets indexing manager.
     *
     * @return Indexing manager.
     */
    public GridIndexingManager indexing();

    /**
     * Gets grid time source.
     *
     * @return Time source.
     */
    public GridClockSource timeSource();

    /**
     * Gets data structures processor.
     *
     * @return Data structures processor.
     */
    public DataStructuresProcessor dataStructures();

    /**
     * Sets segmented flag to {@code true} when node is stopped due to segmentation issues.
     */
    public void markSegmented();

    /**
     * Gets segmented flag.
     *
     * @return {@code True} if network is currently segmented, {@code false} otherwise.
     */
    public boolean segmented();

    /**
     * Print grid kernal memory stats (sizes of internal structures, etc.).
     *
     * NOTE: This method is for testing and profiling purposes only.
     */
    public void printMemoryStats();

    /**
     * Checks whether this node is daemon.
     *
     * @return {@code True} if this node is daemon, {@code false} otherwise.
     */
    public boolean isDaemon();

    /**
     * @return Performance suggestions object.
     */
    public GridPerformanceSuggestions performance();

    /**
     * Gets user version for given class loader by checking
     * {@code META-INF/ignite.xml} file for {@code userVersion} attribute. If
     * {@code ignite.xml} file is not found, or user version is not specified there,
     * then default version (empty string) is returned.
     *
     * @param ldr Class loader.
     * @return User version for given class loader or empty string if no version
     *      was explicitly specified.
     */
    public String userVersion(ClassLoader ldr);

    /**
     * @param name Plugin name.
     * @return Plugin provider instance.
     * @throws PluginNotFoundException If plugin provider for the given name was not found.
     */
    public PluginProvider pluginProvider(String name) throws PluginNotFoundException;

    /**
     * Creates optional component.
     *
     * @param cls Component class.
     * @return Created component.
     */
    public <T> T createComponent(Class<T> cls);

    /**
     * @return Thread pool implementation to be used in grid to process job execution
     *      requests and user messages sent to the node.
     */
    public ExecutorService getExecutorService();

    /**
     * Executor service that is in charge of processing internal system messages.
     *
     * @return Thread pool implementation to be used in grid for internal system messages.
     */
    public ExecutorService getSystemExecutorService();

    /**
     * Executor service that is in charge of processing internal and Visor
     * {@link org.apache.ignite.compute.ComputeJob GridJobs}.
     *
     * @return Thread pool implementation to be used in grid for internal and Visor
     *      jobs processing.
     */
    public ExecutorService getManagementExecutorService();

    /**
     * @return Thread pool implementation to be used for peer class loading
     *      requests handling.
     */
    public ExecutorService getPeerClassLoadingExecutorService();

    /**
     * Executor service that is in charge of processing outgoing GGFS messages.
     *
     * @return Thread pool implementation to be used for GGFS outgoing message sending.
     */
    public ExecutorService getGgfsExecutorService();

    /**
     * Should return an instance of fully configured thread pool to be used for
     * processing of client messages (REST requests).
     *
     * @return Thread pool implementation to be used for processing of client
     *      messages.
     */
    public ExecutorService getRestExecutorService();
}
