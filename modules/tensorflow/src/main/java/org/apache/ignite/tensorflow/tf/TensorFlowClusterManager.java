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

package org.apache.ignite.tensorflow.tf;

import org.apache.ignite.tensorflow.tf.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.tf.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.tf.tfrunning.TensorFlowServer;
import org.apache.ignite.tensorflow.tf.tfrunning.TensorFlowServerManager;
import org.apache.ignite.tensorflow.tf.util.Component;
import org.apache.ignite.tensorflow.tf.util.TensorFlowClusterResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * TensorFlow cluster manager that allows to start, maintain and stop TensorFlow cluster.
 */
public class TensorFlowClusterManager implements Component {
    /** */
    private static final long serialVersionUID = -4847155592164802806L;

    /** TensorFlow cluster metadata cache name. */
    private static final String TF_CLUSTER_METADATA_CACHE_NAME = "TF_CLUSTER_METADATA_CACHE";

    /** TensorFlow server manager. */
    private final TensorFlowServerManager srvProcMgr;

    /** TensorFlow cluster resolver. */
    private final TensorFlowClusterResolver clusterRslvr;

    /** TensorFlow cluster metadata cache. */
    private transient IgniteCache<String, TensorFlowCluster> cache;

    /**
     * Constructs a new instance of TensorFlow cluster manager.
     */
    public TensorFlowClusterManager() {
        this(new TensorFlowServerManager(), new TensorFlowClusterResolver());
    }

    /**
     * Constructs a new instance of TensorFlow cluster manager.
     *
     * @param srvProcMgr TensorFlow server manager.
     * @param clusterRslvr TensorFlow cluster resolver.
     */
    public TensorFlowClusterManager(TensorFlowServerManager srvProcMgr, TensorFlowClusterResolver clusterRslvr) {
        assert srvProcMgr != null : "TensorFlow server manager should not be null";
        assert clusterRslvr != null : "TensorFlow cluster resolver should not be null";

        this.srvProcMgr = srvProcMgr;
        this.clusterRslvr = clusterRslvr;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        clusterRslvr.init();

        CacheConfiguration<String, TensorFlowCluster> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(TF_CLUSTER_METADATA_CACHE_NAME);
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cache = Ignition.ignite().getOrCreateCache(cacheConfiguration);
    }

    /**
     * Creates and starts a new TensorFlow cluster for the specified cache, otherwise returns existing one.
     *
     * @param upstreamCacheName Upstream cache name.
     * @return TensorFlow cluster metadata.
     */
    public TensorFlowCluster getOrCreateCluster(String upstreamCacheName) {
        checkThatInitialized();

        Lock clusterMgrCacheLock = cache.lock(upstreamCacheName);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(upstreamCacheName);

            if (cluster == null) {
                cluster = startCluster(clusterRslvr.resolveAndAcquirePorts(upstreamCacheName));
                cache.put(upstreamCacheName, cluster);
            }

            return cluster;
        }
        finally {
            clusterMgrCacheLock.unlock();
        }
    }

    /**
     * Starts TensorFlow cluster using the specified specification and returns metadata of the started cluster.
     *
     * @param spec TensorFlow cluster specification.
     * @return TensorFlow cluster metadata.
     */
    private TensorFlowCluster startCluster(TensorFlowClusterSpec spec) {
        checkThatInitialized();

        List<TensorFlowServer> srvs = new ArrayList<>();

        Map<String, List<TensorFlowServerAddressSpec>> jobs = spec.getJobs();

        for (String jobName : jobs.keySet()) {
            List<TensorFlowServerAddressSpec> tasks = jobs.get(jobName);

            for (int i = 0; i < tasks.size(); i++) {
                TensorFlowServer srvSpec = new TensorFlowServer(spec, jobName, i);
                srvs.add(srvSpec);
            }
        }

        Map<UUID, List<UUID>> processes = srvProcMgr.start(srvs);

        return new TensorFlowCluster(spec, processes);
    }

    /**
     * Stops TensorFlow cluster.
     *
     * @param upstreamCacheName Upstream cache name.
     */
    public void stopClusterIfExists(String upstreamCacheName) {
        checkThatInitialized();

        Lock clusterMgrCacheLock = cache.lock(upstreamCacheName);
        clusterMgrCacheLock.lock();

        try {
            TensorFlowCluster cluster = cache.get(upstreamCacheName);

            if (cluster != null) {
                srvProcMgr.stop(cluster.getProcesses(), true);
                clusterRslvr.freePorts(cluster.getSpec());
                cache.remove(upstreamCacheName);
                // We need to stop the cluster.
            }
        }
        finally {
            clusterMgrCacheLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        clusterRslvr.destroy();

        Ignition.ignite().destroyCache(TF_CLUSTER_METADATA_CACHE_NAME);
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
    public IgniteCache<String, TensorFlowCluster> getCache() {
        return cache;
    }
}
