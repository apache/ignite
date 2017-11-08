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

package org.apache.ignite.ml.trainers.group;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.jetbrains.annotations.Nullable;

public class GroupTrainerTask<K, S, V, G, U extends Serializable, D> extends ComputeTaskAdapter<Void, U> {
    private final IgniteSupplier<G> contextExtractor;
    private UUID trainingUUID;
    private IgniteFunction<EntryAndContext<K, V, G>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, U>> worker;
    IgniteConsumer<Map<GroupTrainerCacheKey, D>> remoteConsumer;
    // TODO: Also use this reducer on local steps.
    private IgniteBinaryOperator<U> reducer;
    private String cacheName;
    private S data;
    private IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier;
    private IgniteCache<GroupTrainerCacheKey, V> cache;
    private Ignite ignite;

    public GroupTrainerTask(UUID trainingUUID,
        IgniteSupplier<G> ctxExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, U>> remoteWorker,
        IgniteConsumer<Map<GroupTrainerCacheKey, D>> remoteConsumer,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        IgniteBinaryOperator<U> reducer,
        String cacheName,
        S data,
        Ignite ignite) {
        this.trainingUUID = trainingUUID;
        this.contextExtractor = ctxExtractor;
        this.worker = remoteWorker;
        this.remoteConsumer = remoteConsumer;
        this.keysSupplier = keysSupplier;
        this.reducer = reducer;
        this.cacheName = cacheName;
        this.cache = ignite.getOrCreateCache(cacheName);
        this.data = data;
    }

    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Void arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> res = new HashMap<>();

        Set<GroupTrainerCacheKey> keys = keysSupplier.get().
            collect(Collectors.toSet());

        Map<ClusterNode, Collection<GroupTrainerCacheKey>> key2Node = affinity().mapKeysToNodes(keys);

        for (ClusterNode node : subgrid) {
            if (key2Node.get(node).size() > 0)
                res.put(new LocalProcessorJob<>(worker, keysSupplier, reducer, trainingUUID, cacheName, data), node);
        }

        return res;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return super.result(res, rcvd);
    }

    @Nullable @Override
    public U reduce(List<ComputeJobResult> results) throws IgniteException {
        // TODO: safe rewrite.
        return results.stream().map(res -> (U)res.getData()).reduce(reducer).get();
    }

    protected Affinity<GroupTrainerCacheKey> affinity() {
        return ignite.affinity(cacheName);
    }
}
