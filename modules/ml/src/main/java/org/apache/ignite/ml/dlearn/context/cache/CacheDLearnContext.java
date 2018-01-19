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

package org.apache.ignite.ml.dlearn.context.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Learning context based in Ignite Cache storage.
 *
 * @param <P> type of learning context partition
 */
public class CacheDLearnContext<P extends AutoCloseable> implements DLearnContext<P> {
    /** Ignite instance. */
    private final Ignite ignite;

    /** Learning context cache name. */
    private final String learningCtxCacheName;

    /** Partition factory. */
    private final DLearnPartitionFactory<P> partFactory;

    /** Learning context id. */
    private final UUID learningCtxId;

    /** Names of caches which partitions are reserved during computations. */
    private final Collection<String> cacheNames;

    /**
     * Constructs a new instance of cache learning context.
     *
     * @param ignite Ignite instance
     * @param learningCtxCacheName learning context cache name
     * @param partFactory partition factory
     * @param learningCtxId learning context id
     * @param cacheNames names of caches which partitions are reserved during computations
     */
    public CacheDLearnContext(Ignite ignite, String learningCtxCacheName, DLearnPartitionFactory<P> partFactory,
        UUID learningCtxId, Collection<String> cacheNames) {
        this.ignite = ignite;
        this.learningCtxCacheName = learningCtxCacheName;
        this.partFactory = partFactory;
        this.learningCtxId = learningCtxId;
        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer, R identity) {
        ClusterGroup clusterGrp = ignite.cluster().forDataNodes(learningCtxCacheName);

        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);

        Affinity<?> affinity = ignite.affinity(learningCtxCacheName);

        List<IgniteFuture<R>> futures = new ArrayList<>(affinity.partitions());

        for (int part = 0; part < affinity.partitions(); part++) {
            int currPart = part;
            IgniteFuture<R> fut = ignite.compute(clusterGrp).affinityCallAsync(
                cacheNames,
                part,
                () -> {
                    DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, learningCtxId, currPart);

                    P learningCtxPart = partFactory.createPartition(storage);

                    return mapper.apply(learningCtxPart, currPart);
                }
            );
            futures.add(fut);
        }

        List<R> results = new ArrayList<>(affinity.partitions());
        for (IgniteFuture<R> future : futures)
            results.add(future.get());

        return reduce(results, reducer, identity);
    }

    /** {@inheritDoc} */
    @Override public void compute(IgniteBiConsumer<P, Integer> mapper) {
        ClusterGroup clusterGrp = ignite.cluster().forDataNodes(learningCtxCacheName);

        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);

        Affinity<?> affinity = ignite.affinity(learningCtxCacheName);

        List<IgniteFuture<Void>> futures = new ArrayList<>(affinity.partitions());

        for (int part = 0; part < affinity.partitions(); part++) {
            int currPart = part;
            IgniteFuture<Void> fut = ignite.compute(clusterGrp).affinityRunAsync(
                cacheNames,
                part,
                () -> {
                    DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, learningCtxId, currPart);

                    P learningCtxPart = partFactory.createPartition(storage);

                    mapper.accept(learningCtxPart, currPart);
                }
            );
            futures.add(fut);
        }

        for (IgniteFuture<Void> future : futures)
            future.get();
    }

    /** {@inheritDoc} */
    @Override public <T extends AutoCloseable, C extends DLearnContext<T>> C transform(
        DLearnContextTransformer<P, T, C> transformer) {
        UUID newLearnCtxId = UUID.randomUUID();

        compute((part, partIdx) -> {
            IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);
            DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, newLearnCtxId, partIdx);

            T newPart = transformer.createPartition(storage);

            transformer.transform(part, newPart);
        });

        DLearnContext<T> newCtx = new CacheDLearnContext<>(ignite, learningCtxCacheName, transformer, newLearnCtxId,
            Collections.singletonList(learningCtxCacheName));

        return transformer.wrapContext(newCtx);
    }

    /**
     * Reduces results into a single final result.
     *
     * @param results results
     * @param reducer reducer function
     * @param identity identity
     * @param <R> type of result
     * @return single final result
     */
    private <R> R reduce(Collection<R> results, IgniteBinaryOperator<R> reducer, R identity) {
        R res = identity;

        for (R partRes : results)
            res = reducer.apply(res, partRes);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        compute(this::closePartition);
    }

    /**
     * Closes partition.
     *
     * @param part partition to be closed
     */
    private void closePartition(P part) {
        try {
            part.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public String getLearningCtxCacheName() {
        return learningCtxCacheName;
    }
}
