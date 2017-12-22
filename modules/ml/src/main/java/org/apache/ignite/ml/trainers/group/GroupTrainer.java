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
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Class encapsulating synchronous distributed group training.
 *
 * @param <LC> Type of local context of the training.
 * @param <K> Type of cache keys on which the training is done.
 * @param <V> Type of cache values on which the training is done.
 * @param <IN> Type of data returned after initializing of distributed context.
 * @param <R> Type of result returned after training from each node.
 * @param <I> Type of data which is fed into each training loop step and returned from it.
 * @param <M> Type of model returned after training.
 * @param <T> Type of input to this trainer.
 * @param <G> Type of distributed context which is needed for forming final result which is send from each node to trainer for final model creation.
 */
public abstract class GroupTrainer<LC extends HasTrainingUUID, K, V, IN extends Serializable, R extends Serializable, I extends Serializable, M extends Model, T extends Distributive<K>, G> implements Trainer<M, T> {
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
        LC locCtx = initialLocalContext(data, trainingUUID);

        GroupTrainingContext<K, V, LC> ctx = new GroupTrainingContext<>(locCtx, new CacheContext<>(cache), ignite);
        ComputationsChain<LC, K, V, T, T> chain = (i, c) -> i;

        M res = chain.
            thenDistributedForKeys(this::initDistributed, (t, lc) -> () -> data.initialKeys(trainingUUID), this::reduceDistributedInitData).
            thenLocally(this::locallyProcessInitData).
            thenWhile(this::shouldContinue, trainingLoopStep()).
            thenDistributedForEntries(this::extractContextForModelCreation, this::getFinalResults, Functions.outputSupplier(this::finalResultKeys), defaultFinalResult(), this::reduceFinalResults).
            thenLocally(this::mapFinalResult).
            process(data, ctx);

        cleanup(locCtx);

        return res;
    }

    protected abstract LC initialLocalContext(T data, UUID trainingUUID);

    protected abstract ResultAndUpdates<IN> initDistributed(T data, GroupTrainerCacheKey<K> key);

    protected abstract IN reduceDistributedInitData(IN data1, IN data2);

    protected abstract I locallyProcessInitData(IN data, LC locCtx);

    protected abstract ComputationsChain<LC, K, V, I, I> trainingLoopStep();

    protected abstract boolean shouldContinue(I data, LC locCtx);

    protected abstract G extractContextForModelCreation(I data, LC locCtx);

    protected abstract Stream<GroupTrainerCacheKey<K>> finalResultKeys(I data, LC locCtx);

    protected abstract ResultAndUpdates<R> getFinalResults(I data, LC locCtx, EntryAndContext<K, V, G> entryAndCtx);

    protected abstract R defaultFinalResult();

    protected abstract R reduceFinalResults(R res1, R res2);

    protected abstract M mapFinalResult(R res, LC locCtx);

    protected abstract void cleanup(LC locCtx);
}
