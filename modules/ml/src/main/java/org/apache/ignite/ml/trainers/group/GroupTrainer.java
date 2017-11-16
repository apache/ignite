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
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.trainers.Trainer;
import org.apache.ignite.ml.trainers.group.chain.CacheContext;
import org.apache.ignite.ml.trainers.group.chain.DistributedTrainerWorkersChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

public abstract class GroupTrainer<LC, K, V, IR extends Serializable, R extends Serializable, I extends Serializable, M extends Model, T extends Distributive<K>, G> implements Trainer<M, T> {
    IgniteCache<GroupTrainerCacheKey<K>, V> cache;
    Ignite ignite;

    public GroupTrainer(
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        this.cache = cache;
        this.ignite = ignite;
    }

    @Override public M train(T data) {
        UUID trainingUUID = UUID.randomUUID();
        LC locCtx = initialLocalContext(data);

        GroupTrainingContext<K, V, LC> ctx = new GroupTrainingContext<>(locCtx, trainingUUID, new CacheContext<>(cache), ignite);
        DistributedTrainerWorkersChain<LC, K, V, T, GroupTrainingContext<K, V, LC>, T> chain = (i, c) -> i;

        M res = chain.
            thenDistributed(this::initGlobal, (t, lc) -> data::initialKeys, this::reduceGlobalInitData).
            thenLocally(this::processInitData).
            thenWhile(this::shouldContinue, trainingLoopStep()).
            thenDistributed(this::extractContextForModelCreation, this::getFinalResults, Functions.outputSupplier(this::finalResultKeys), defaultFinalResult(), this::reduceFinalResults).
            thenLocally(this::mapFinalResult).
            process(data, ctx);

        cleanup();

        return res;
    }

    protected abstract LC initialLocalContext(T data);

    protected abstract ResultAndUpdates<IR> initGlobal(T data, GroupTrainerCacheKey<K> key);

    protected abstract IR reduceGlobalInitData(IR data1, IR data2);

    protected abstract I processInitData(IR data, LC locCtx);

    protected abstract DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, I> trainingLoopStep();

    protected abstract boolean shouldContinue(I data, LC locCtx);

    protected abstract G extractContextForModelCreation(I data, LC locCtx);

    protected abstract Stream<GroupTrainerCacheKey<K>> finalResultKeys(I data, LC locCtx);

    protected abstract ResultAndUpdates<R> getFinalResults(I data, LC locCtx, EntryAndContext<K, V, G> entryAndCtx);

    protected abstract R defaultFinalResult();

    protected abstract R reduceFinalResults(R res1, R res2);

    protected abstract M mapFinalResult(R res, LC locCtx);

    protected abstract void cleanup();
}
