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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;

/**
 * Management bean that provides access to {@link CacheGroupContext}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public class CacheGroupMetricsMXBeanImpl implements CacheGroupMetricsMXBean {
    /** Cache group context. */
    private final CacheGroupContext ctx;

    /** Cache group metrics. */
    private final CacheGroupMetricsImpl metrics;

    /**
     * Creates Group metrics MBean.
     *
     * @param ctx Cache group context.
     */
    public CacheGroupMetricsMXBeanImpl(CacheGroupContext ctx) {
        this.ctx = ctx;
        this.metrics = ctx.metrics();
    }

    /** {@inheritDoc} */
    @Override public int getGroupId() {
        return ctx.groupId();
    }

    /** {@inheritDoc} */
    @Override public String getGroupName() {
        return ctx.name();
    }

    /** {@inheritDoc} */
    @Override public List<String> getCaches() {
        List<String> caches = new ArrayList<>(ctx.caches().size());

        for (GridCacheContext cache : ctx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /** {@inheritDoc} */
    @Override public int getBackups() {
        return ctx.config().getBackups();
    }

    /** {@inheritDoc} */
    @Override public int getPartitions() {
        return ctx.topology().partitions();
    }

    /** {@inheritDoc} */
    @Override public int getMinimumNumberOfPartitionCopies() {
        return metrics.getMinimumNumberOfPartitionCopies();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumNumberOfPartitionCopies() {
        return metrics.getMaximumNumberOfPartitionCopies();
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeOwningPartitionsCount() {
        return metrics.getLocalNodeOwningPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeMovingPartitionsCount() {
        return metrics.getLocalNodeMovingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeRentingPartitionsCount() {
        return metrics.getLocalNodeRentingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getLocalNodeRentingEntriesCount() {
        return metrics.getLocalNodeRentingEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public int getClusterOwningPartitionsCount() {
        return metrics.getClusterOwningPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public int getClusterMovingPartitionsCount() {
        return metrics.getClusterMovingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getOwningPartitionsAllocationMap() {
        return metrics.getOwningPartitionsAllocationMap();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getMovingPartitionsAllocationMap() {
        return metrics.getMovingPartitionsAllocationMap();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap() {
        return metrics.getAffinityPartitionsAssignmentMap();
    }

    /** {@inheritDoc} */
    @Override public String getType() {
        CacheMode type = ctx.config().getCacheMode();

        return String.valueOf(type);
    }

    /** {@inheritDoc} */
    @Override public List<Integer> getPartitionIds() {
        return metrics.getPartitionIds();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return metrics.getTotalAllocatedPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return metrics.getTotalAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long getStorageSize() {
        return metrics.getStorageSize();
    }

    /** {@inheritDoc} */
    @Override public long getSparseStorageSize() {
        return metrics.getSparseStorageSize();
    }

    /** {@inheritDoc} */
    @Override public long getIndexBuildCountPartitionsLeft() {
        return metrics.getIndexBuildCountPartitionsLeft();
    }
}
