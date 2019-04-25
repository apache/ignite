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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Collect list of lost partitions.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheLostPartitionsTask
    extends VisorOneNodeTask<VisorCacheLostPartitionsTaskArg, VisorCacheLostPartitionsTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheLostPartitionsJob job(VisorCacheLostPartitionsTaskArg arg) {
        return new VisorCacheLostPartitionsJob(arg, debug);
    }

    /**
     * Job that collect list of lost partitions.
     */
    private static class VisorCacheLostPartitionsJob
        extends VisorJob<VisorCacheLostPartitionsTaskArg, VisorCacheLostPartitionsTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Object with list of cache names to collect lost partitions.
         * @param debug Debug flag.
         */
        private VisorCacheLostPartitionsJob(VisorCacheLostPartitionsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheLostPartitionsTaskResult run(VisorCacheLostPartitionsTaskArg arg) {
            Map<String, List<Integer>> res = new HashMap<>();

            for (String cacheName: arg.getCacheNames()) {
                IgniteInternalCache cache = ignite.cachex(cacheName);

                if (cache != null) {
                    GridDhtPartitionTopology top = cache.context().topology();

                    List<Integer> lostPartitions = new ArrayList<>(top.lostPartitions());

                    if (!lostPartitions.isEmpty())
                        res.put(cacheName, lostPartitions);
                }
            }

            return new VisorCacheLostPartitionsTaskResult(res);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheLostPartitionsJob.class, this);
        }
    }
}
