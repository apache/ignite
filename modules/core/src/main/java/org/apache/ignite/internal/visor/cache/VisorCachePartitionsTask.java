/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.escapeName;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.log;

/**
 * Task that collect keys distribution in partitions.
 */
@GridInternal
public class VisorCachePartitionsTask extends VisorMultiNodeTask<VisorCachePartitionsTaskArg,
    Map<UUID, VisorCachePartitions>, VisorCachePartitions> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachePartitionsJob job(VisorCachePartitionsTaskArg arg) {
        return new VisorCachePartitionsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, VisorCachePartitions> reduce0(List<ComputeJobResult> results) {
        Map<UUID, VisorCachePartitions> parts = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            parts.put(res.getNode().id(), res.getData());
        }

        return parts;
    }

    /**
     * Job that collect cache metrics from node.
     */
    private static class VisorCachePartitionsJob extends VisorJob<VisorCachePartitionsTaskArg, VisorCachePartitions> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Tasks arguments.
         * @param debug Debug flag.
         */
        private VisorCachePartitionsJob(VisorCachePartitionsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCachePartitions run(VisorCachePartitionsTaskArg arg) throws IgniteException {
            String cacheName = arg.getCacheName();

            if (debug)
                log(ignite.log(), "Collecting partitions for cache: " + escapeName(cacheName));

            VisorCachePartitions parts = new VisorCachePartitions();

            GridCacheAdapter ca = ignite.context().cache().internalCache(cacheName);

            // Cache was not started.
            if (ca == null || !ca.context().started() || VisorTaskUtils.isRestartingCache(ignite, cacheName))
                return parts;

            CacheConfiguration cfg = ca.configuration();

            CacheMode mode = cfg.getCacheMode();

            boolean partitioned = (mode == CacheMode.PARTITIONED || mode == CacheMode.REPLICATED)
                && ca.context().affinityNode();

            if (partitioned) {
                GridDhtCacheAdapter dca = null;

                if (ca instanceof GridNearCacheAdapter)
                    dca = ((GridNearCacheAdapter)ca).dht();
                else if (ca instanceof GridDhtCacheAdapter)
                    dca = (GridDhtCacheAdapter)ca;

                if (dca != null) {
                    GridDhtPartitionTopology top = dca.topology();

                    List<GridDhtLocalPartition> locParts = top.localPartitions();

                    for (GridDhtLocalPartition part : locParts) {
                        int p = part.id();

                        long sz = part.dataStore().cacheSize(ca.context().cacheId());

                        // Pass NONE as topology version in order not to wait for topology version.
                        if (part.primary(AffinityTopologyVersion.NONE))
                            parts.addPrimary(p, sz);
                        else if (part.state() == GridDhtPartitionState.OWNING && part.backup(AffinityTopologyVersion.NONE))
                            parts.addBackup(p, sz);
                    }
                }
            }

            return parts;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachePartitionsJob.class, this);
        }
    }
}
