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

package org.apache.ignite.internal.processors.query.stat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Estimates cache size using the currently available local primary partitions on each data node.
 * Nodes need not agree on a topology version: missed or double-counted rows during rebalance are acceptable
 * for planning statistics. An incomplete request returns {@code null} so the previous estimate can be retained.
 */
@GridInternal
public class StatisticsCacheSizeTask extends ComputeTaskAdapter<Void, Long> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String cacheName;

    /** @param cacheName Cache name. */
    public StatisticsCacheSizeTask(String cacheName) {
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg) {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new SizeJob(cacheName), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        return WAIT;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Long reduce(List<ComputeJobResult> results) {
        long size = 0;

        for (ComputeJobResult res : results) {
            Long locSize = res.getData();

            if (res.getException() != null || locSize == null)
                return null;

            size += locSize;
        }

        return size;
    }

    /** Reads local counters directly, bypassing cache proxy initialization and topology readiness waits. */
    private static class SizeJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        private final String cacheName;

        /** @param cacheName Cache name. */
        SizeJob(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object execute() {
            GridCacheAdapter<?, ?> cache = ignite.context().cache().internalCache(cacheName);

            if (cache == null || !cache.context().started())
                return null;

            try {
                return cache.localSizeLong(new CachePeekMode[] {PRIMARY});
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }
}
