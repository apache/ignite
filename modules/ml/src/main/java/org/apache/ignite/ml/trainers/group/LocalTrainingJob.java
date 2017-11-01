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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.util.StreamUtil;

public class LocalTrainingJob<S, U extends Serializable, G, D> implements ComputeJob {
    private IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, U>> worker;
    private UUID trainingUUID;
    private String cacheName;
    private Ignite ignite;
    private IgniteCache<GroupTrainerCacheKey, G> cache;
    private S data;
    private IgniteSupplier<Stream<Integer>> keySupplier;
    private IgniteBinaryOperator<U> reducer;

    public LocalTrainingJob(IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, U>> worker,
        IgniteSupplier<Stream<Integer>> keySupplier,
        IgniteBinaryOperator<U> reducer,
        UUID trainingUUID, String cacheName,
        S data) {
        this.worker = worker;
        this.keySupplier = keySupplier;
        this.reducer = reducer;
        this.trainingUUID = trainingUUID;
        this.cacheName = cacheName;
        this.data = data;

        ignite = Ignition.localIgnite();
        cache = ignite.getOrCreateCache(cacheName);
    }

    @Override public void cancel() {

    }

    @Override public U execute() throws IgniteException {
        Map<GroupTrainerCacheKey, G> m = new ConcurrentHashMap<>();

        List<IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, U>> res = selectLocalEntries().
            parallel().
            map(entry -> worker.apply(entry, data)).collect(Collectors.toList());


        // TODO: use remote consumer
//        res.stream().map(IgniteBiTuple::get1).forEach(r -> m.put(r.getKey(), r.getValue()));
//
//        cache.putAll(m);

        return res.stream().map(IgniteBiTuple::get2).reduce(reducer).get();
    }

    // TODO: do it more optimally.
    private Stream<Map.Entry<GroupTrainerCacheKey, G>> selectLocalEntries() {
        Set<GroupTrainerCacheKey> keys = keySupplier.get().
            map(k -> new GroupTrainerCacheKey(k, trainingUUID)).
            filter(k -> affinity().mapKeyToNode(k).isLocal()).
            collect(Collectors.toSet());

        return cache.getAll(keys).entrySet().stream();
    }

    private Affinity<GroupTrainerCacheKey> affinity() {
        return ignite.affinity(cacheName);
    }
}
