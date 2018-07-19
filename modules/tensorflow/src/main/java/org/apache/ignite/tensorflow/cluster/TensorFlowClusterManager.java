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

package org.apache.ignite.tensorflow.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServer;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerManager;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowChiefRunner;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowClusterResolver;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowUserScriptRunner;

/**
 * TensorFlow cluster manager that allows to start, maintain and stop TensorFlow cluster.
 */
public class TensorFlowClusterManager implements Serializable {
    /** */
    private static final long serialVersionUID = -4847155592164802806L;

    /** TensorFlow cluster metadata cache name. */
    private static final String TF_CLUSTER_METADATA_CACHE_NAME = "TF_CLUSTER_METADATA_CACHE";

    /** Ignite instance supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /** TensorFlow server manager. */
    private final TensorFlowServerManager srvProcMgr;

    /** TensorFlow cluster resolver. */
    private final TensorFlowClusterResolver clusterRslvr;

    /** TensorFlow cluster metadata cache. */
    private transient IgniteCache<UUID, TensorFlowCluster> cache;

    /** TensorFlow chief runners. */
    private transient ConcurrentMap<UUID, TensorFlowChiefRunner> chiefRunners;

    /** TensorFlow user script runners. */
    private transient ConcurrentMap<UUID, TensorFlowUserScriptRunner> userScriptRunners;

    /**
     * Constructs a new instance of TensorFlow cluster manager.
     *
     * @param igniteSupplier Ignite instance supplier.
     * @param <T> Type of serializable supplier.
     */
    public <T extends Supplier<Ignite> & Serializable> TensorFlowClusterManager(T igniteSupplier) {
        this(
            igniteSupplier,
            new TensorFlowServerManager(igniteSupplier),
            new TensorFlowClusterResolver(igniteSupplier)
        );
    }

    /**
     * Constructs a new instance of TensorFlow cluster manager.
     *
     * @param igniteSupplier Ignite instance supplier.
     * @param srvProcMgr TensorFlow server manager.
     * @param clusterRslvr TensorFlow cluster resolver.
     */
    public <T extends Supplier<Ignite> & Serializable> TensorFlowClusterManager(T igniteSupplier,
        TensorFlowServerManager srvProcMgr, TensorFlowClusterResolver clusterRslvr) {
        assert igniteSupplier != null : "Ignite supplier should not be null";
        assert srvProcMgr != null : "TensorFlow server manager should not be null";
        assert clusterRslvr != null : "TensorFlow cluster resolver should not be null";

        this.igniteSupplier = igniteSupplier;
        this.srvProcMgr = srvProcMgr;
        this.clusterRslvr = clusterRslvr;
    }

    /** Initializes TensorFlow cluster manager and gets or creates correspondent caches. */
    public void init() {
        clusterRslvr.init();

        CacheConfiguration<UUID, TensorFlowCluster> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(TF_CLUSTER_METADATA_CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        Ignite ignite = igniteSupplier.get();
        cache = ignite.getOrCreateCache(cacheConfiguration);

        chiefRunners = new ConcurrentHashMap<>();
        userScriptRunners = new ConcurrentHashMap<>();
    }

    /**
     * Creates and starts a new TensorFlow cluster for the specified cache if it doesn't exist, otherwise returns
     * existing one.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @return TensorFlow cluster metadata.
     */
    public TensorFlowCluster getOrCreateCluster(UUID clusterId, TensorFlowJobArchive jobArchive, IgniteRunnable cb) {
        checkThatInitialized();

        Lock clusterMgrCacheLock = cache.lock(clusterId);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(clusterId);

            if (cluster == null) {
                TensorFlowClusterSpec clusterSpec = clusterRslvr.resolveAndAcquirePorts(jobArchive.getUpstreamCacheName());
                cluster = startCluster(clusterId, clusterSpec, jobArchive, cb);
                cache.put(clusterId, cluster);
            }

            return cluster;
        }
        finally {
            clusterMgrCacheLock.unlock();
        }
    }

    /**
     * Stops TensorFlow cluster.
     *
     * @param clusterId TensorFlow cluster identifier.
     */
    public void stopClusterIfExists(UUID clusterId) {
        checkThatInitialized();

        Lock clusterMgrCacheLock = cache.lock(clusterId);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(clusterId);

            if (cluster != null) {
                stopChief(clusterId);
                stopUserScript(clusterId);
                srvProcMgr.stop(cluster.getProcesses(), true);
                clusterRslvr.freePorts(cluster.getSpec());
                cache.remove(clusterId);
            }
        }
        finally {
            clusterMgrCacheLock.unlock();
        }
    }

    /** Destroys TensorFlow cluster manager and related caches. */
    public void destroy() {
        clusterRslvr.destroy();

        Ignite ignite = igniteSupplier.get();
        ignite.destroyCache(TF_CLUSTER_METADATA_CACHE_NAME);
    }

    /**
     * Starts TensorFlow cluster using the specified specification and returns metadata of the started cluster.
     *
     * @param clusterId Cluster identifier.
     * @param spec TensorFlow cluster specification.
     * @return TensorFlow cluster metadata.
     */
    private TensorFlowCluster startCluster(UUID clusterId, TensorFlowClusterSpec spec, TensorFlowJobArchive jobArchive, IgniteRunnable cb) {
        Map<String, List<TensorFlowServerAddressSpec>> jobs = spec.getJobs();

        Map<UUID, List<UUID>> workerProcesses = startWorkers(spec, jobs.get(clusterRslvr.getWorkerJobName()));

        startChief(clusterId, spec);
        startUserScript(clusterId, jobArchive, spec, cb);

        return new TensorFlowCluster(spec, workerProcesses);
    }

    /**
     * Starts TensorFlow worker processes using the specified specification and returns identifiers of the started
     * processes.
     *
     * @param spec TensorFlow cluster specification.
     * @param tasks Worker tasks.
     * @return Identifiers of the started processes.
     */
    private Map<UUID, List<UUID>> startWorkers(TensorFlowClusterSpec spec, List<TensorFlowServerAddressSpec> tasks) {
        List<TensorFlowServer> srvs = new ArrayList<>();

        if (tasks != null) {
            for (int i = 0; i < tasks.size(); i++) {
                TensorFlowServer srvSpec = new TensorFlowServer(spec, clusterRslvr.getWorkerJobName(), i);
                srvs.add(srvSpec);
            }
        }

        return srvProcMgr.start(srvs);
    }

    /**
     * Starts chief process using the specified cluster specification.
     *
     * @param clusterId Cluster identifier.
     * @param spec TensorFlow cluster specification.
     */
    private void startChief(UUID clusterId, TensorFlowClusterSpec spec) {
        TensorFlowChiefRunner chiefRunner = new TensorFlowChiefRunner(spec);
        chiefRunner.start();

        chiefRunners.put(clusterId, chiefRunner);
    }

    /**
     * Starts user script processes using the specified job archive.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @param clusterSpec Cluster specification.
     */
    private void startUserScript(UUID clusterId, TensorFlowJobArchive jobArchive, TensorFlowClusterSpec clusterSpec,
        IgniteRunnable cb) {
        TensorFlowUserScriptRunner userScriptRunner = new TensorFlowUserScriptRunner(jobArchive, clusterSpec, cb);
        userScriptRunner.start("stdout_" + clusterId, "stderr_" + clusterId);

        userScriptRunners.put(clusterId, userScriptRunner);
    }

    /**
     * Stops chief process.
     *
     * @param clusterId Cluster identifier.
     */
    private void stopChief(UUID clusterId) {
        TensorFlowChiefRunner runner = chiefRunners.remove(clusterId);

        if (runner != null)
            runner.stop();
    }

    /**
     * Stops user script process.
     *
     * @param clusterId Cluster identifier.
     */
    private void stopUserScript(UUID clusterId) {
        TensorFlowUserScriptRunner runner = userScriptRunners.remove(clusterId);

        if (runner != null)
            runner.stop();
    }

    /**
     * Checks that the component has been initialized.
     */
    private void checkThatInitialized() {
        if (cache == null)
            throw new IllegalStateException("TensorFlow Cluster Manager is not initialized");
    }

    /** */
    public TensorFlowServerManager getSrvProcMgr() {
        return srvProcMgr;
    }

    /** */
    public IgniteCache<UUID, TensorFlowCluster> getCache() {
        return cache;
    }
}
