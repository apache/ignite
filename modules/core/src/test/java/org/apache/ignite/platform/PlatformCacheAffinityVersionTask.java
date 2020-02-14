/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Task retrieves a affinity topology version for cache.
 */
public class PlatformCacheAffinityVersionTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        String arg) throws IgniteException {
        return Collections.singletonMap(new Job(arg), subgrid.stream().filter(ClusterNode::isLocal).findFirst().get());
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class Job extends ComputeJobAdapter {
        /** Ignite instance. */
        @IgniteInstanceResource
        protected transient Ignite ignite;

        /**
         * Cache name.
         */
        private String cacheName;

        /**
         * @param cacheName Cache name.
         */
        public Job(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            AffinityTopologyVersion topVer = ((IgniteEx)ignite).context().cache().cache(cacheName).context().group()
                .topology().readyTopologyVersion();

            return new Long[] {topVer.topologyVersion(), (long)topVer.minorTopologyVersion()};
        }
    }
}
