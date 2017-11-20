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

public class LocalEntriesProcessorJob<K, V, G, O extends Serializable> extends BaseLocalProcessorJob<K, V, EntryAndContext<K, V, G>, O> {
    private final IgniteSupplier<G> contextExtractor;

    public LocalEntriesProcessorJob(IgniteSupplier<G> contextExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        O identity,
        IgniteBinaryOperator<O> reducer,
        UUID trainingUUID, String cacheName) {
        super(worker, keySupplier, identity, reducer, trainingUUID, cacheName);
        this.contextExtractor = contextExtractor;
    }

    @Override protected Stream<EntryAndContext<K, V, G>> toProcess() {
        G ctx = contextExtractor.get();
        cache = ignite().getOrCreateCache(cacheName);

        return selectLocalEntries().
            //parallel().
            map(e -> new EntryAndContext<>(e, ctx));
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
