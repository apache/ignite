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
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Class encapsulating synchronous distributed group training.
 * Training is performed by following scheme:
 * 1. For specified set of keys distributed initialization is done. For each key some initialization result is returned.
 * 2. All initialization results are processed locally and reduced into some object of type I.
 * 3. While 'shouldContinue' condition is true, training loop step is executed.
 * 4. After loop is finished, data from each key from final key set is collected.
 * 5. Data collected on previous step is transformed into a model which is returned as final result.
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
public abstract class GroupTrainer<LC extends HasTrainingUUID, K, V, IN extends Serializable, R extends Serializable, I extends Serializable, M extends Model, T extends GroupTrainerInput<K>, G> implements Trainer<M, T> {
    /**
     * Cache on which training is performed. For example it can be cache of neural networks.
     */
    IgniteCache<GroupTrainerCacheKey<K>, V> cache;

    /**
     * Ignite instance.
     */
    Ignite ignite;

    /**
     * Construct an instance of this class.
     *
     * @param cache Cache on which training is performed.
     * @param ignite Ignite instance.
     */
    public GroupTrainer(
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        this.cache = cache;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public M train(T data) {
        UUID trainingUUID = UUID.randomUUID();
        LC locCtx = initialLocalContext(data, trainingUUID);

        GroupTrainingContext<K, V, LC> ctx = new GroupTrainingContext<>(locCtx, cache, ignite);
        ComputationsChain<LC, K, V, T, T> chain = (i, c) -> i;

        M res = chain.
            thenDistributedForKeys(this::initDistributed, (t, lc) -> () -> data.initialKeys(trainingUUID), this::reduceDistributedInitData).
            thenLocally(this::locallyProcessInitData).
            thenWhile(this::shouldContinue, trainingLoopStep()).
            thenDistributedForEntries(this::extractContextForFinalResultCreation, this::getFinalResults, Functions.outputSupplier(this::finalResultKeys), this::reduceFinalResults, defaultFinalResult()).
            thenLocally(this::mapFinalResult).
            process(data, ctx);

        cleanup(locCtx);

        return res;
    }

    /**
     * Create initial local context from data given as input to trainer.
     *
     * @param data Data given as input to this trainer.
     * @param trainingUUID UUID of this training.
     * @return Initial local context.
     */
    protected abstract LC initialLocalContext(T data, UUID trainingUUID);

    /**
     * Do initialization for each of keys specified in initial key set.
     *
     * @param data Data given to this trainer as input.
     * @param key Type of keys of cache on which training is performed.
     * @return ResultAndUpdates object. Results are then accumulated and reduced, updates are performed on corresponding caches.
     */
    protected abstract ResultAndUpdates<IN> initDistributed(T data, GroupTrainerCacheKey<K> key);

    /**
     * Reduces data from initialization of each key specified in initial key set.
     *
     * @param data1 First operand of reducer.
     * @param data2 Second operand of reducer.
     * @return Result of reducing.
     */
    protected abstract IN reduceDistributedInitData(IN data1, IN data2);

    /**
     * Transform data from initialization step into data which is fed as input to first step of training loop.
     *
     * @param data Data from initialization step.
     * @param locCtx Local context.
     * @return Data which is fed as input to first step of training loop.
     */
    protected abstract I locallyProcessInitData(IN data, LC locCtx);

    /**
     * Training loop step.
     *
     * @return Result of training loop step.
     */
    protected abstract ComputationsChain<LC, K, V, I, I> trainingLoopStep();

    /**
     * Condition specifying if training loop should continue.
     *
     * @param data First time, data returned by locallyProcessInitData then data returned by last step of loop.
     * @param locCtx Local context.
     * @return Boolean value indicating if training loop should continue.
     */
    protected abstract boolean shouldContinue(I data, LC locCtx);

    /**
     * Extract context for final result creation. Each key from the final keys set will be processed with
     * getFinalResults. While entry data (i.e. key and value) for each key varies, some data can be common for all
     * processed entries. This data is called context.
     *
     * @param data Data returned from last training loop step.
     * @param locCtx Local context.
     * @return Context.
     */
    protected abstract G extractContextForFinalResultCreation(I data, LC locCtx);

    /**
     * Keys for final result creation.
     *
     * @param data Data returned from the last training loop step.
     * @param locCtx Local context.
     * @return Stream of keys for final result creation.
     */
    protected abstract Stream<GroupTrainerCacheKey<K>> finalResultKeys(I data, LC locCtx);

    /**
     * Get final result from each key specified in finalResultKeys.
     *
     * @param entryAndCtx Cache entry for given key and context returned by extractContextForFinalResultCreation.
     * @return ResultAndUpdates object.
     */
    protected abstract ResultAndUpdates<R> getFinalResults(EntryAndContext<K, V, G> entryAndCtx);

    /**
     * Default final result. Should be identity for reduceFinalResults.
     *
     * @return Default final result.
     */
    protected abstract R defaultFinalResult();

    /**
     * Function for reducing final results.
     *
     * @param res1 First reducer argument.
     * @param res2 Second reducer argument.
     * @return Result of reducing.
     */
    protected abstract R reduceFinalResults(R res1, R res2);

    /**
     * Map final result to model which is returned by trainer.
     *
     * @param res Final result.
     * @param locCtx Local context.
     * @return Model resulted from training.
     */
    protected abstract M mapFinalResult(R res, LC locCtx);

    protected abstract void cleanup(LC locCtx);
}
