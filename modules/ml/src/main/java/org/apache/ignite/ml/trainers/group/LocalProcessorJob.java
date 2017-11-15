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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

public class LocalProcessorJob<K, V, G, O extends Serializable> implements ComputeJob {
    private final IgniteSupplier<G> contextExtractor;
    private final O identity;
    private IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O>> worker;
    private UUID trainingUUID;
    private String cacheName;
    private IgniteCache<GroupTrainerCacheKey<K>, V> cache;
    private IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier;
    private IgniteBinaryOperator<O> reducer;

    public LocalProcessorJob(IgniteSupplier<G> contextExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        O identity,
        IgniteBinaryOperator<O> reducer,
        UUID trainingUUID, String cacheName) {
        this.contextExtractor = contextExtractor;
        this.worker = worker;
        this.keySupplier = keySupplier;
        this.identity = identity;
        this.reducer = reducer;
        this.trainingUUID = trainingUUID;
        this.cacheName = cacheName;
    }

    @Override public void cancel() {

    }

    @Override public O execute() throws IgniteException {
        cache = ignite().getOrCreateCache(cacheName);
        G ctx = contextExtractor.get();

        List<ResultAndUpdates<O>> resultsAndUpdates = selectLocalEntries().parallel().
            map(e -> new EntryAndContext<>(e, ctx)).
            map(e -> worker.apply(e)).
            collect(Collectors.toList());

        ResultAndUpdates<O> totalRes = ResultAndUpdates.sum(reducer, identity, resultsAndUpdates);

        totalRes.processUpdates(ignite());

        return totalRes.result();
    }

    private static Ignite ignite() {
        return Ignition.localIgnite();
    }

    // TODO: do it more optimally.
    private Stream<Map.Entry<GroupTrainerCacheKey<K>, V>> selectLocalEntries() {
        Set<GroupTrainerCacheKey<K>> keys = keySupplier.get().
            filter(k -> affinity().mapKeyToNode(k).isLocal()).
            filter(k -> k.trainingUUID().equals(trainingUUID)).
            collect(Collectors.toSet());

        return cache.getAll(keys).entrySet().stream();
    }

    private Affinity<GroupTrainerCacheKey> affinity() {
        return ignite().affinity(cacheName);
    }
}
