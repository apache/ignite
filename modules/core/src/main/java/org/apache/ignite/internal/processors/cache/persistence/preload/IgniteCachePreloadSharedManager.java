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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.GridTopic.TOPIC_REBALANCE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/**
 *
 */
public class IgniteCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    static final int REBALANCE_TOPIC_IDX = 0;

    /** */
    private final boolean presistenceRebalanceEnabled;

    /** */
    private GridPartitionDownloadManager downloadMgr;

    /** */
    private GridPartitionUploadManager uploadMgr;

    /**
     * @param ktx Kernal context.
     */
    public IgniteCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";

        downloadMgr = new GridPartitionDownloadManager(ktx);
        uploadMgr = new GridPartitionUploadManager(ktx);

        presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
            IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);
    }

    /**
     * @return The Rebalance topic to communicate with.
     */
    static Object rebalanceThreadTopic() {
        return TOPIC_REBALANCE.topic("Rebalance", REBALANCE_TOPIC_IDX);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        downloadMgr.start0(cctx);
        uploadMgr.start0(cctx);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        downloadMgr.stop0(cancel);
        uploadMgr.stop0(cancel);
    }

    /**
     * @return {@code True} if rebalance via sending partitions files enabled. Default <tt>false</tt>.
     */
    public boolean isPresistenceRebalanceEnabled() {
        return presistenceRebalanceEnabled;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        return rebalanceByPartitionSupported(grp, assigns.keySet());
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param topVer Topology versions to calculate assignmets at.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, AffinityTopologyVersion topVer) {
        AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

        // All of affinity nodes must support to new persistence rebalance feature.
        List<ClusterNode> affNodes =  aff.idealAssignment().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return rebalanceByPartitionSupported(grp, affNodes);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes The list of nodes to check ability of file transferring.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    private boolean rebalanceByPartitionSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean partitionRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        return rebalanceByPartitionSupported(grp, assigns) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @return The instantiated download manager.
     */
    public GridPartitionDownloadManager download() {
        return downloadMgr;
    }

    /**
     *
     * @return The instantiated upload mamanger.
     */
    public GridPartitionUploadManager upload() {
        return uploadMgr;
    }
}
