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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Learning context based in Ignite Cache storage.
 *
 * @param <P> type of learning context partition
 */
public class CacheDLearnContext<P> implements DLearnContext<P> {
    /** */
    private final Ignite ignite;

    /** */
    private final String learningCtxCacheName;

    /** */
    private final DLearnPartitionFactory<P> partFactory;

    /** */
    private final UUID learningCtxId;

    /** */
    public CacheDLearnContext(Ignite ignite, String learningCtxCacheName, DLearnPartitionFactory<P> partFactory, UUID learningCtxId) {
        this.ignite = ignite;
        this.learningCtxCacheName = learningCtxCacheName;
        this.partFactory = partFactory;
        this.learningCtxId = learningCtxId;
    }

    /** */
    public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer) {
        ClusterGroup clusterGrp = ignite.cluster().forDataNodes(learningCtxCacheName);

        Collection<R> results = ignite.compute(clusterGrp).broadcast(() -> {
            IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);

            Affinity<?> affinity = ignite.affinity(learningCtxCacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            int[] partitions = affinity.primaryPartitions(locNode);
            R res = null;
            for (int part : partitions) {
                DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, learningCtxId, part);
                P learningCtxPart = partFactory.createPartition(storage);
                R partRes = mapper.apply(learningCtxPart, part);
                res = reducer.apply(res, partRes);
            }
            return res;
        });

        return reduce(results, reducer);
    }

    /** */
    @Override public void compute(IgniteBiConsumer<P, Integer> mapper) {
        ClusterGroup clusterGrp = ignite.cluster().forDataNodes(learningCtxCacheName);

        ignite.compute(clusterGrp).broadcast(() -> {
            IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);

            Affinity<?> affinity = ignite.affinity(learningCtxCacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            int[] partitions = affinity.primaryPartitions(locNode);
            for (int part : partitions) {
                DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, learningCtxId, part);
                P learningCtxPart = partFactory.createPartition(storage);
                mapper.accept(learningCtxPart, part);
            }
        });
    }

    /** */
    @Override public <T, C extends DLearnContext<T>> C transform(DLearnContextTransformer<P, T, C> transformer) {
        UUID newLearningCtxId = UUID.randomUUID();

        compute((part, partIdx) -> {
            IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(learningCtxCacheName);
            DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, newLearningCtxId, partIdx);
            T newPart = transformer.createPartition(storage);
            transformer.transform(part, newPart);
        });

        DLearnContext<T> newCtx = new CacheDLearnContext<>(ignite, learningCtxCacheName, transformer, newLearningCtxId);

        return transformer.wrapContext(newCtx);
    }

    /** */
    private <R> R reduce(Collection<R> results, IgniteBinaryOperator<R> reducer) {
        R res = null;
        for (R partRes : results)
            res = reducer.apply(res, partRes);
        return res;
    }
}