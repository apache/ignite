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

package org.apache.ignite.yardstick.cache;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
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
