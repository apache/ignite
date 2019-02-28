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
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;

/** */
public class IgniteCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    private final boolean presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);

    /** */
    private GridPartitionDownloadManager downloadMgr;

    /** */
    private GridPartitionUploadManager uploadMgr;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        downloadMgr = new GridPartitionDownloadManager(cctx.kernalContext());
        uploadMgr = new GridPartitionUploadManager(cctx.kernalContext());

        downloadMgr.start0();
        uploadMgr.start0();
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

    /** */
    public GridPartitionDownloadManager download() {
        return downloadMgr;
    }

    /** */
    public GridPartitionUploadManager upload() {
        return uploadMgr;
    }
}
