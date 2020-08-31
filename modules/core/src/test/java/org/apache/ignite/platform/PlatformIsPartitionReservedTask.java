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

package org.apache.ignite.platform;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to get Java thread names.
 */
public class PlatformIsPartitionReservedTask extends ComputeTaskAdapter<Object[], Boolean> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object[] arg) {
        //noinspection OptionalGetWithoutIsPresent
        ClusterNode localNode = subgrid.stream().filter(ClusterNode::isLocal).findFirst().get();

        return Collections.singletonMap(
                new PlatformIsPartitionReservedJob((String)arg[0], (Integer)arg[1]), localNode);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class PlatformIsPartitionReservedJob extends ComputeJobAdapter {
        /** */
        private final String cacheName;

        /** */
        private final int part;

        /** */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         *
         * @param cacheName Cache name.
         * @param part Partition.
         */
        public PlatformIsPartitionReservedJob(String cacheName, Integer part) {
            this.cacheName = cacheName;
            this.part = part;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Boolean execute() {
            GridKernalContext ctx = ((IgniteEx) ignite).context();

            GridDhtPartitionTopology top = ctx.cache().cache(cacheName).context().topology();

            GridDhtLocalPartition locPart = top.localPartition(part, top.readyTopologyVersion(), false);

            assert locPart != null;

            return locPart.reservations() > 0;
        }
    }
}
