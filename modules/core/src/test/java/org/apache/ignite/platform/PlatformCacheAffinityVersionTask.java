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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Task retrieves a affinity topology version for cache.
 */
public class PlatformCacheAffinityVersionTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        String arg) throws IgniteException {

        return Collections.singletonMap(new Job(arg), F.find(subgrid, null,
            (IgnitePredicate<? super ClusterNode>)ClusterNode::isLocal));
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
