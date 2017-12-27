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
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.DistributedEntryProcessingStep;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Distributed step
 *
 * @param <L>
 * @param <K>
 * @param <V>
 * @param <G>
 * @param <I>
 * @param <O>
 * @param <X>
 * @param <Y>
 * @param <D>
 */
class MetaoptimizerDistributedStep<L extends HasTrainingUUID, K, V, G, I extends Serializable, O extends Serializable, X, Y, D extends Serializable> implements DistributedEntryProcessingStep<L,K,V,G,I,O> {
    /**
     * {@link Metaoptimizer}.
     */
    private final Metaoptimizer<L, X, Y, I, D, O> metaoptimizer;

    /**
     * {@link MetaoptimizerGroupTrainer} for which this distributed step is used.
     */
    private final MetaoptimizerGroupTrainer<L, K, V, D, ?, I, ?, ?, G, O, X, Y> trainer;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param metaoptimizer Metaoptimizer.
     * @param trainer {@link MetaoptimizerGroupTrainer} for which this distributed step is used.
     */
    public MetaoptimizerDistributedStep(Metaoptimizer<L, X, Y, I, D, O> metaoptimizer,
        MetaoptimizerGroupTrainer<L, K, V, D, ?, I, ?, ?, G, O, X, Y> trainer) {
        this.metaoptimizer = metaoptimizer;
        this.trainer = trainer;
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<G> remoteContextSupplier(I input, L locCtx) {
        return trainer.remoteContextExtractor(input, locCtx);
    }

    /** {@inheritDoc} */
    @Override public IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O>> worker() {
        IgniteFunction<X, ResultAndUpdates<Y>> dataProcessor = trainer.dataProcessor();
        IgniteFunction<X, X> preprocessor = metaoptimizer.distributedPreprocessor();
        IgniteFunction<Y, O> postprocessor = metaoptimizer.distributedPostprocessor();
        IgniteFunction<EntryAndContext<K, V, G>, X> ctxExtractor = trainer.trainingLoopStepDataExtractor();

        return entryAndCtx -> {
            X apply = ctxExtractor.apply(entryAndCtx);
            preprocessor.apply(apply);
            ResultAndUpdates<Y> res = dataProcessor.apply(apply);
            O postprocessRes = postprocessor.apply(res.result());

            return ResultAndUpdates.of(postprocessRes).setUpdates(res.updates());
        };
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keys(I input, L locCtx) {
        return trainer.keysToProcessInTrainingLoop(locCtx);
    }

    /** {@inheritDoc} */
    @Override public O identity() {
        return metaoptimizer.postProcessIdentity();
    }

    /** {@inheritDoc} */
    @Override public IgniteBinaryOperator<O> reducer() {
        return metaoptimizer.postProcessReducer();
    }
}
