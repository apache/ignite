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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;

/**
 * A task for finishing preloading future in exchange worker thread.
 */
public class FinishPreloadingTask implements CachePartitionExchangeWorkerTask {
    /**
     * Topology version.
     */
    private final AffinityTopologyVersion topVer;

    /**
     * Group id.
     */
    private final int grpId;

    /**
     * @param topVer Topology version.
     */
    public FinishPreloadingTask(AffinityTopologyVersion topVer, int grpId) {
        this.grpId = grpId;
        this.topVer = topVer;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }
}
