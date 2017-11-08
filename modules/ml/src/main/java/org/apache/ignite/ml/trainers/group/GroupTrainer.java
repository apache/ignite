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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteCurriedBiFunction;
import org.apache.ignite.ml.trainers.Trainer;
import org.apache.ignite.ml.trainers.group.chain.CacheContext;
import org.apache.ignite.ml.trainers.group.chain.DC;
import org.apache.ignite.ml.trainers.group.chain.DistributedTrainerWorkersChain;
import org.apache.ignite.ml.trainers.group.chain.HasCacheContext;

public abstract class GroupTrainer<LC, K, V, I, O, R, M extends Model, T> implements Trainer<M, T> {
    IgniteCurriedBiFunction<Integer, T, O> init;
    IgniteCurriedBiFunction<Integer, I, O> worker;
    IgniteFunction<I, IgniteBiTuple<LC, R>> handler;
    IgnitePredicate<Collection<O>> stopper;
    IgniteFunction<Integer, R> result;
    IgniteFunction<Collection<R>, M> modelProducer;
    IgniteCache<GroupTrainerCacheKey<K>, V> cache;
    int nodeLocalEntitiesCount;
    Ignite ignite;

    public GroupTrainer(
        IgniteCurriedBiFunction<Integer, T, O> init,
        IgniteCurriedBiFunction<Integer, I, O> worker,
        IgniteFunction<I, IgniteBiTuple<LC, R>> handler,
        IgnitePredicate<Collection<O>> stopper,
        IgniteFunction<Integer, R> result,
        IgniteFunction<Collection<R>, M> modelProducer,
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        int nodeLocEntitiesCnt,
        Ignite ignite) {
        this.init = init;
        this.worker = worker;
        this.handler = handler;
        this.stopper = stopper;
        this.result = result;
        this.modelProducer = modelProducer;
        this.cache = cache;
        this.nodeLocalEntitiesCount = nodeLocEntitiesCnt;
        this.ignite = ignite;
    }

    @Override public M train(T data) {
        UUID trainingUUID = UUID.randomUUID();
        GroupTrainingContext<K, V, LC> ctx = new GroupTrainingContext<>(initLocalContext(data), trainingUUID, new CacheContext<>(cache), ignite);
        DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, I> chain = (i, c) -> i;

        chain.
            thenDistributed().
            thenDistributed().process()

        initGlobalContext(data, trainingUUID);

        return null;
    }

    protected abstract LC initLocalContext(T data);

    protected abstract I initGlobalContext(T data, UUID trainingUUID);

    protected abstract DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, I> trainingLoop();

    private <A, B> B execute(ComputeTask<A, B> task, A arg) {
        return ignite.compute(ignite.cluster().forCacheNodes(cache.getName())).execute(task, arg);
    }
}
