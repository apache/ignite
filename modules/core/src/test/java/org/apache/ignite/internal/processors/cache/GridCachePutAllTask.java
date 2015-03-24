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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Puts all the passed data into partitioned cache in small chunks.
 */
class GridCachePutAllTask extends ComputeTaskAdapter<Collection<Integer>, Void> {
    /** Number of entries per put. */
    private static final int TX_BOUND = 30;

    /** Preferred node. */
    private final UUID preferredNode;

    /** Cache name. */
    private final String cacheName;

    /**
     *
     * @param preferredNode A node that we'd prefer to take from grid.
     * @param cacheName A name of the cache to work with.
     */
    GridCachePutAllTask(UUID preferredNode, String cacheName) {
        this.preferredNode = preferredNode;
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable final Collection<Integer> data) {
        assert !subgrid.isEmpty();

        // Give preference to wanted node. Otherwise, take the first one.
        ClusterNode targetNode = F.find(subgrid, subgrid.get(0), new IgnitePredicate<ClusterNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(ClusterNode e) {
                return preferredNode.equals(e.id());
            }
        });

        return Collections.singletonMap(
            new ComputeJobAdapter() {
                @LoggerResource
                private IgniteLogger log;

                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object execute() {
                    log.info("Going to put data: " + data);

                    IgniteCache<Object, Object> cache = ignite.cache(cacheName);

                    assert cache != null;

                    HashMap<Integer, Integer> putMap = U.newLinkedHashMap(TX_BOUND);

                    Iterator<Integer> it = data.iterator();

                    int cnt = 0;

                    while (it.hasNext()) {
                        Integer val = it.next();

                        putMap.put(val, val);

                        if (++cnt == TX_BOUND) {
                            log.info("Putting keys to cache: " + putMap.keySet());

                            cache.putAll(putMap);

                            cnt = 0;

                            putMap = U.newLinkedHashMap(TX_BOUND);
                        }
                    }

                    assert cnt < TX_BOUND;
                    assert putMap.size() == (data.size() % TX_BOUND) : "putMap.size() = " + putMap.size();

                    log.info("Putting keys to cache: " + putMap.keySet());

                    cache.putAll(putMap);

                    log.info("Finished putting data: " + data);

                    return data;
                }
            },
            targetNode);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        if (res.getException() != null)
            return ComputeJobResultPolicy.FAILOVER;

        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
        return null;
    }
}
