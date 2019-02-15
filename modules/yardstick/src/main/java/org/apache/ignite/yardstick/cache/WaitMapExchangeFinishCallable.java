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

package org.apache.ignite.yardstick.cache;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public class WaitMapExchangeFinishCallable implements IgniteCallable<Void> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Void call() throws Exception {
        Collection<IgniteInternalCache<?, ?>> cachesx = ((IgniteKernal)ignite).cachesx(null);

        for (IgniteInternalCache<?, ?> cache : cachesx) {
            try {
                GridDhtPartitionTopology top = cache.context().isNear() ? cache.context().near().dht().topology() :
                    cache.context().dht().topology();

                BenchmarkUtils.println("Validating cache: " + cache.name());

                for (;;) {
                    boolean success = true;

                    if (top.readyTopologyVersion().topologyVersion() == ignite.cluster().topologyVersion()) {
                        for (Map.Entry<UUID, GridDhtPartitionMap> e : top.partitionMap(true).entrySet()) {
                            for (Map.Entry<Integer, GridDhtPartitionState> p : e.getValue().entrySet()) {
                                if (p.getValue() != GridDhtPartitionState.OWNING) {
                                    BenchmarkUtils.println("Not owning partition [part=" + p.getKey() +
                                        ", state=" + p.getValue() + ']');

                                    success = false;

                                    break;
                                }
                            }

                            if (!success)
                                break;
                        }
                    }
                    else {
                        BenchmarkUtils.println("Topology version is different [cache=" + top.readyTopologyVersion() +
                            ", cluster=" + ignite.cluster().topologyVersion() + ']');

                        success = false;
                    }

                    if (!success)
                        Thread.sleep(1000);
                    else {
                        BenchmarkUtils.println("Cache state is fine: " + cache.name());

                        break;
                    }
                }
            }
            catch (RuntimeException e1) {
                BenchmarkUtils.println("Ignored exception: " + e1);
            }
        }

        return null;
    }
}
