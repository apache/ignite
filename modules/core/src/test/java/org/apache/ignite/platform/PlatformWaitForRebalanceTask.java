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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task that waits for rebalance to complete.
 */
public class PlatformWaitForRebalanceTask extends ComputeTaskAdapter<Object[], Boolean> {
    /** {@inheritDoc} */
    @NotNull
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object[] args)
            throws IgniteException {
        return Collections.singletonMap(new Job(args), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class Job extends ComputeJobAdapter {
        /** Expected version. */
        private final AffinityTopologyVersion topVer;

        /** Cache name. */
        private final String cacheName;

        /** Timeout. */
        private final long timeout;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Ctor.
         *
         * @param args Args.
         */
        private Job(Object[] args) {
            cacheName = (String)args[0];
            topVer = new AffinityTopologyVersion((Long)args[1], (Integer)args[2]);
            timeout = (Long)args[3];
        }

        /** {@inheritDoc} */
        @Override public Boolean execute() throws IgniteException {
            final GridDhtPartitionTopology top =
                    ((IgniteEx)ignite).context().cache().context().cacheContext(CU.cacheId(cacheName)).topology();

            try {

                return GridTestUtils.waitForCondition(() -> {
                    ignite.log().info("PlatformWaitForRebalanceTask.Job: Waiting for rebalance to complete [expectedTopVer="
                            + topVer + ", readyTopVer=" + top.readyTopologyVersion() + ']');

                    return top.rebalanceFinished(topVer);
                }, timeout);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
