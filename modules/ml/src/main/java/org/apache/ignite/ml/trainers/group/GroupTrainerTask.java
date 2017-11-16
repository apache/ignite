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
import java.util.UUID;
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
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.jetbrains.annotations.Nullable;

public class GroupTrainerTask<K, S, V, G, U extends Serializable> extends ComputeTaskAdapter<Void, U> {
    private final IgniteSupplier<G> contextExtractor;
    private final UUID trainingUUID;
    private IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<U>> worker;
    private final U identity;
    // TODO: Also use this reducer on local steps.
    private final IgniteBinaryOperator<U> reducer;
    private final String cacheName;
    private final S data;
    private final IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier;
    private final IgniteCache<GroupTrainerCacheKey, V> cache;
    private final Ignite ignite;

    public GroupTrainerTask(UUID trainingUUID,
        IgniteSupplier<G> ctxExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<U>> remoteWorker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        U identity,
        IgniteBinaryOperator<U> reducer,
        String cacheName,
        S data,
        Ignite ignite) {
        this.trainingUUID = trainingUUID;
        this.contextExtractor = ctxExtractor;
        this.worker = remoteWorker;
        this.keysSupplier = keysSupplier;
        this.identity = identity;
        this.reducer = reducer;
        this.cacheName = cacheName;
        this.cache = ignite.getOrCreateCache(cacheName);
        this.data = data;
        this.ignite = ignite;
    }

    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Void arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> res = new HashMap<>();

        for (ClusterNode node : subgrid)
            res.put(new LocalProcessorJob<>(contextExtractor, worker, keysSupplier, identity, reducer, trainingUUID, cacheName), node);

        return res;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return super.result(res, rcvd);
    }

    @Nullable @Override
    public U reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.stream().map(res -> (U)res.getData()).reduce(reducer).orElse(identity);
    }

    protected Affinity<GroupTrainerCacheKey> affinity() {
        return ignite.affinity(cacheName);
    }
}
