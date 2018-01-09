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
import org.apache.ignite.ml.trainers.group.chain.Chains;
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Group trainer using {@link Metaoptimizer}.
 * Main purpose of this trainer is to extract various transformations (normalizations for example) of data which is processed
 * in the training loop step into distinct entity called metaoptimizer and only fix the main part of logic in
 * trainers extending this class. This way we'll be able to quickly switch between this transformations by using different metaoptimizers
 * without touching main logic.
 *
 * @param <LC> Type of local context.
 * @param <K> Type of data in {@link GroupTrainerCacheKey} keys on which the training is done.
 * @param <V> Type of values of cache used in group training.
 * @param <IN> Data type which is returned by distributed initializer.
 * @param <R> Type of final result returned by nodes on which training is done.
 * @param <I> Type of data which is fed into each training loop step and returned from it.
 * @param <M> Type of model returned after training.
 * @param <T> Type of input of this trainer.
 * @param <G> Type of distributed context which is needed for forming final result which is send from each node to trainer for final model creation.
 * @param <O> Type of output of postprocessor.
 * @param <X> Type of data which is processed by dataProcessor.
 * @param <Y> Type of data which is returned by postprocessor.
 */
public abstract class MetaoptimizerGroupTrainer<LC extends HasTrainingUUID, K, V, IN extends Serializable,
    R extends Serializable, I extends Serializable,
    M extends Model, T extends GroupTrainerInput<K>,
    G, O extends Serializable, X, Y> extends
    GroupTrainer<LC, K, V, IN, R, I, M, T, G> {
    /**
     * Metaoptimizer.
     */
    private Metaoptimizer<LC, X, Y, I, IN, O> metaoptimizer;

    /**
     * Construct instance of this class.
     *
     * @param cache Cache on which group trainer is done.
     * @param ignite Ignite instance.
     */
    public MetaoptimizerGroupTrainer(Metaoptimizer<LC, X, Y, I, IN, O> metaoptimizer,
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        super(cache, ignite);
        this.metaoptimizer = metaoptimizer;
    }

    /**
     * Get function used to map EntryAndContext to type which is processed by dataProcessor.
     *
     * @return Function used to map EntryAndContext to type which is processed by dataProcessor.
     */
    protected abstract IgniteFunction<EntryAndContext<K, V, G>, X> trainingLoopStepDataExtractor();

    /**
     * Get supplier of keys which should be processed by training loop.
     *
     * @param locCtx Local text.
     * @return Supplier of keys which should be processed by training loop.
     */
    protected abstract IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysToProcessInTrainingLoop(LC locCtx);

    /**
     * Get supplier of context used in training loop step.
     *
     * @param input Input.
     * @param ctx Local context.
     * @return Supplier of context used in training loop step.
     */
    protected abstract IgniteSupplier<G> remoteContextExtractor(I input, LC ctx);

    /** {@inheritDoc} */
    @Override protected void init(T data, UUID trainingUUID) {
    }

    /**
     * Get function used to process data in training loop step.
     *
     * @return Function used to process data in training loop step.
     */
    protected abstract IgniteFunction<X, ResultAndUpdates<Y>> dataProcessor();

    /** {@inheritDoc} */
    @Override protected ComputationsChain<LC, K, V, I, I> trainingLoopStep() {
        ComputationsChain<LC, K, V, I, O> chain = Chains.create(new MetaoptimizerDistributedStep<>(metaoptimizer, this));
        return chain.thenLocally(metaoptimizer::localProcessor);
    }

    /** {@inheritDoc} */
    @Override protected I locallyProcessInitData(IN data, LC locCtx) {
        return metaoptimizer.locallyProcessInitData(data, locCtx);
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldContinue(I data, LC locCtx) {
        return metaoptimizer.shouldContinue(data, locCtx);
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<List<IN>, IN> reduceDistributedInitData() {
        return metaoptimizer.initialReducer();
    }
}
