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
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
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
 * Note that all methods returning functions, suppliers etc should return values with minimal dependencies because they are serialized
 * with all dependent objects.
 *
 * @param <LC> Type of local context of the training.
 * @param <K> Type of data in {@link GroupTrainerCacheKey} keys on which the training is done.
 * @param <V> Type of cache values on which the training is done.
 * @param <IN> Type of data returned after initializing of distributed context.
 * @param <R> Type of result returned after training from each node.
 * @param <I> Type of data which is fed into each training loop step and returned from it.
 * @param <M> Type of model returned after training.
 * @param <T> Type of input to this trainer.
 * @param <G> Type of distributed context which is needed for forming final result which is send from each node to trainer for final model creation.
 */
abstract class GroupTrainer<LC extends HasTrainingUUID, K, V, IN extends Serializable, R extends Serializable, I extends Serializable, M extends Model, T extends GroupTrainerInput<K>, G> implements Trainer<M, T> {
    /**
     * Cache on which training is performed. For example it can be cache of neural networks.
     */
    protected IgniteCache<GroupTrainerCacheKey<K>, V> cache;

    /**
     * Ignite instance.
     */
    protected Ignite ignite;

    /**
     * Construct an instance of this class.
     *
     * @param cache Cache on which training is performed.
     * @param ignite Ignite instance.
     */
    GroupTrainer(
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        this.cache = cache;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public final M train(T data) {
        UUID trainingUUID = UUID.randomUUID();
        LC locCtx = initialLocalContext(data, trainingUUID);

        GroupTrainingContext<K, V, LC> ctx = new GroupTrainingContext<>(locCtx, cache, ignite);
        ComputationsChain<LC, K, V, T, T> chain = (i, c) -> i;
        IgniteFunction<GroupTrainerCacheKey<K>, ResultAndUpdates<IN>> distributedInitializer
            = distributedInitializer(data);

        init(data, trainingUUID);

        M res = chain.
            thenDistributedForKeys(distributedInitializer, (t, lc) -> data.initialKeys(trainingUUID),
                reduceDistributedInitData()).
            thenLocally(this::locallyProcessInitData).
            thenWhile(this::shouldContinue, trainingLoopStep()).
            thenDistributedForEntries(this::extractContextForFinalResultCreation, finalResultsExtractor(),
                this::finalResultKeys, finalResultsReducer()).
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

    /** Override in subclasses if needed. */
    protected void init(T data, UUID trainingUUID) {
    }

    /**
     * Get function for initialization for each of keys specified in initial key set.
     *
     * @param data Data given to this trainer as input.
     * @return Function for initialization for each of keys specified in initial key set.
     */
    protected abstract IgniteFunction<GroupTrainerCacheKey<K>, ResultAndUpdates<IN>> distributedInitializer(T data);

    /**
     * Get reducer to reduce data collected from initialization of each key specified in initial key set.
     *
     * @return Reducer to reduce data collected from initialization of each key specified in initial key set.
     */
    protected abstract IgniteFunction<List<IN>, IN> reduceDistributedInitData();

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
     * finalResultsExtractor. While entry data (i.e. key and value) for each key varies, some data can be common for all
     * processed entries. This data is called context.
     *
     * @param data Data returned from last training loop step.
     * @param locCtx Local context.
     * @return Context.
     */
    protected abstract IgniteSupplier<G> extractContextForFinalResultCreation(I data, LC locCtx);

    /**
     * Keys for final result creation.
     *
     * @param data Data returned from the last training loop step.
     * @param locCtx Local context.
     * @return Stream of keys for final result creation.
     */
    protected abstract IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> finalResultKeys(I data, LC locCtx);

    /**
     * Get function for extracting final result from each key specified in finalResultKeys.
     *
     * @return Function for extracting final result from each key specified in finalResultKeys.
     */
    protected abstract IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<R>> finalResultsExtractor();

    /**
     * Get function for reducing final results.
     *
     * @return Function for reducing final results.
     */
    protected abstract IgniteFunction<List<R>, R> finalResultsReducer();

    /**
     * Map final result to model which is returned by trainer.
     *
     * @param res Final result.
     * @param locCtx Local context.
     * @return Model resulted from training.
     */
    protected abstract M mapFinalResult(R res, LC locCtx);

    /**
     * Performs cleanups of temporary objects created by this trainer.
     *
     * @param locCtx Local context.
     */
    protected abstract void cleanup(LC locCtx);
}
