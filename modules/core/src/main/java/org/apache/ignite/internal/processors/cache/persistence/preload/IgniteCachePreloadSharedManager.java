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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
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
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grp The corresponding to assignments cache group context.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(assigns.keySet(), IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /** */
    public GridPartitionDownloadManager download() {
        return downloadMgr;
    }

    /** */
    public GridPartitionUploadManager upload() {
        return uploadMgr;
    }
}
