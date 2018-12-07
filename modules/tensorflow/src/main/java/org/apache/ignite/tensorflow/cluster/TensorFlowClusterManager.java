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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServer;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerManager;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowChiefRunner;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowClusterResolver;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowUserScriptRunner;
import org.apache.ignite.tensorflow.core.util.CustomizableThreadFactory;

/**
 * TensorFlow cluster manager that allows to start, maintain and stop TensorFlow cluster.
 */
public class TensorFlowClusterManager {
    /** TensorFlow cluster metadata cache name. */
    private static final String TF_CLUSTER_METADATA_CACHE_NAME = "TF_CLUSTER_METADATA_CACHE";

    /** Ignite instance. */
    private final Ignite ignite;

    /** TensorFlow server manager. */
    private final TensorFlowServerManager srvProcMgr;

    /** TensorFlow cluster resolver. */
    private final TensorFlowClusterResolver clusterRslvr;

    /** TensorFlow cluster metadata cache. */
    private IgniteCache<UUID, TensorFlowCluster> cache;

    /** TensorFlow chief runners. */
    private ConcurrentMap<UUID, TensorFlowChiefRunner> chiefRunners;

    /** TensorFlow user script runners. */
    private ConcurrentMap<UUID, TensorFlowUserScriptRunner> userScriptRunners;

    /**
     * Constructs a new instance of TensorFlow cluster manager.
     *
     * @param ignite Ignite instance.
     */
    public TensorFlowClusterManager(Ignite ignite) {
        assert ignite != null : "Ignite instance should not be null";

        this.ignite = ignite;
        this.srvProcMgr = new TensorFlowServerManager(ignite);
        this.clusterRslvr = new TensorFlowClusterResolver(ignite, "TF", 10000, 1000);
        this.cache = getOrCreateCache();
        this.chiefRunners = new ConcurrentHashMap<>();
        this.userScriptRunners = new ConcurrentHashMap<>();
    }

    /**
     * Returns cluster by identifier.
     *
     * @param clusterId Cluster identifier.
     * @return TensorFlow cluster.
     */
    public TensorFlowCluster getCluster(UUID clusterId) {
        return cache.get(clusterId);
    }

    /**
     * Creates and starts a new TensorFlow cluster for the specified cache.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @return TensorFlow cluster metadata.
     */
    public TensorFlowCluster createCluster(UUID clusterId, TensorFlowJobArchive jobArchive,
        Consumer<String> userScriptOut, Consumer<String> userScriptErr) {
        Lock clusterMgrCacheLock = cache.lock(clusterId);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(clusterId);

            if (cluster != null)
                throw new IllegalStateException("Cluster is already created [clusterId=" + clusterId + "]");

            TensorFlowClusterSpec clusterSpec = clusterRslvr.resolveAndAcquirePorts(jobArchive.getUpstreamCacheName());
            cluster = startCluster(clusterId, clusterSpec, jobArchive, userScriptOut, userScriptErr);
            cache.put(clusterId, cluster);

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
        Lock clusterMgrCacheLock = cache.lock(clusterId);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(clusterId);

            if (cluster != null) {
                stopChief(clusterId);
                stopUserScript(clusterId);
                srvProcMgr.stop(cluster.getProcesses(), true);
                clusterRslvr.releasePorts(cluster.getSpec());
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
        ignite.destroyCache(TF_CLUSTER_METADATA_CACHE_NAME);
    }

    /**
     * Starts TensorFlow cluster using the specified specification and returns metadata of the started cluster.
     *
     * @param clusterId Cluster identifier.
     * @param spec TensorFlow cluster specification.
     * @return TensorFlow cluster metadata.
     */
    private TensorFlowCluster startCluster(UUID clusterId, TensorFlowClusterSpec spec, TensorFlowJobArchive jobArchive,
        Consumer<String> userScriptOut, Consumer<String> userScriptErr) {
        Map<String, List<TensorFlowServerAddressSpec>> jobs = spec.getJobs();

        Map<UUID, List<UUID>> workerProcesses = startWorkers(spec, jobs.get(TensorFlowClusterResolver.WORKER_JOB_NAME));

        startChief(clusterId, spec);
        startUserScript(clusterId, jobArchive, spec, userScriptOut, userScriptErr);

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
                TensorFlowServer srvSpec = new TensorFlowServer(spec, TensorFlowClusterResolver.WORKER_JOB_NAME, i);
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
        TensorFlowChiefRunner chiefRunner = new TensorFlowChiefRunner(
            ignite,
            Executors.newSingleThreadExecutor(
                new CustomizableThreadFactory("tf-ch", true)
            ),
            spec,
            System.out::println,
            System.err::println
        );

        chiefRunner.start();

        chiefRunners.put(clusterId, chiefRunner);
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
     * Checks if chief completed and returns result.
     *
     * @param clusterId Cluster identifier.
     * @return {@code true} if chief completed, otherwise {@code false}.
     */
    public boolean isChiefCompleted(UUID clusterId) {
        TensorFlowChiefRunner runner = chiefRunners.get(clusterId);

        return runner != null && runner.isCompleted();
    }

    /**
     * Returns an exception that happened during execution or {@code null} if there is no exception.
     *
     * @param clusterId Cluster identifier.
     * @return Exception that happened during execution or {@code null} if there is no exception.
     */
    public Exception getChiefException(UUID clusterId) {
        TensorFlowChiefRunner runner = chiefRunners.get(clusterId);

        return runner != null ? runner.getException() : null;
    }

    /**
     * Starts user script processes using the specified job archive.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @param clusterSpec Cluster specification.
     */
    private void startUserScript(UUID clusterId, TensorFlowJobArchive jobArchive, TensorFlowClusterSpec clusterSpec,
        Consumer<String> out, Consumer<String> err) {
        TensorFlowUserScriptRunner userScriptRunner = new TensorFlowUserScriptRunner(
            ignite,
            Executors.newSingleThreadExecutor(
                new CustomizableThreadFactory("tf-us", true)
            ),
            jobArchive,
            clusterSpec,
            out,
            err
        );

        userScriptRunner.start();

        userScriptRunners.put(clusterId, userScriptRunner);
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
     * Checks if user script completed and returns result.
     *
     * @param clusterId Cluster identifier.
     * @return {@code true} if user script completed, otherwise {@code false}.
     */
    public boolean isUserScriptCompleted(UUID clusterId) {
        TensorFlowUserScriptRunner runner = userScriptRunners.get(clusterId);

        return runner != null && runner.isCompleted();
    }

    /**
     * Returns an exception that happened during execution or {@code null} if there is no exception.
     *
     * @param clusterId Cluster identifier.
     * @return Exception that happened during execution or {@code null} if there is no exception.
     */
    public Exception getUserScriptException(UUID clusterId) {
        TensorFlowUserScriptRunner runner = userScriptRunners.get(clusterId);

        return runner != null ? runner.getException() : null;
    }

    /**
     * Returns list of maintained TensorFlow clusters.
     *
     * @return List of maintained TensorFlow clusters.
     */
    public Map<UUID, TensorFlowCluster> getAllClusters() {
        Map<UUID, TensorFlowCluster> res = new HashMap<>();

        ScanQuery<UUID, TensorFlowCluster> qry = new ScanQuery<>();
        QueryCursor<Cache.Entry<UUID, TensorFlowCluster>> cursor = cache.query(qry);
        for (Cache.Entry<UUID, TensorFlowCluster> e : cursor)
            res.put(e.getKey(), e.getValue());

        return res;
    }

    /** */
    public TensorFlowServerManager getSrvProcMgr() {
        return srvProcMgr;
    }

    /**
     * Returns existing cluster manager cache or creates a new one.
     *
     * @return Cluster manager cache.
     */
    private IgniteCache<UUID, TensorFlowCluster> getOrCreateCache() {
        CacheConfiguration<UUID, TensorFlowCluster> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(TF_CLUSTER_METADATA_CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return ignite.getOrCreateCache(cacheConfiguration);
    }
}
