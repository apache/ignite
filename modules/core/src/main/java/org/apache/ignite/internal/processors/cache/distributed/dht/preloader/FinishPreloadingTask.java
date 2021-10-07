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
import org.apache.ignite.internal.processors.cache.AbstractCachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.security.SecurityContext;

/**
 * A task for finishing preloading future in exchange worker thread.
 */
public class FinishPreloadingTask extends AbstractCachePartitionExchangeWorkerTask {
    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Group id. */
    private final int grpId;

    /** Rebalance id. */
    private final long rebalanceId;

    /**
     * @param secCtx Security context in which current task must be executed.
     * @param topVer Topology version.
     */
    public FinishPreloadingTask(SecurityContext secCtx, AffinityTopologyVersion topVer, int grpId, long rebalanceId) {
        super(secCtx);

        this.grpId = grpId;
        this.topVer = topVer;
        this.rebalanceId = rebalanceId;
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

    /**
     * @return Rebalance id.
     */
    public long rebalanceId() {
        return rebalanceId;
    }
}
