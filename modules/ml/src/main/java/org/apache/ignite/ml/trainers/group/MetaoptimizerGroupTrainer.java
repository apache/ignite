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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.Chains;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

public abstract class MetaoptimizerGroupTrainer<LC extends HasTrainingUUID, K, V, D extends Serializable,
R extends Serializable, I extends Serializable,
M extends Model, T extends GroupTrainerInput<K>,
G, O extends Serializable, IR, X, Y> extends
    GroupTrainer<LC, K, V, D, R, I, M, T, G> {
    private Metaoptimizer<IR, LC, X, Y, I, D, O> metaoptimizer;

    public MetaoptimizerGroupTrainer(Metaoptimizer<IR, LC, X, Y, I, D, O> metaoptimizer,
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        super(cache, ignite);
        this.metaoptimizer = metaoptimizer;
    }

    abstract X extractDataToProcessInTrainingLoop(EntryAndContext<K, V, G> entryAndCtx);

    protected abstract IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysToProcessInTrainingLoop(LC locCtx);

    protected abstract IgniteSupplier<G> extractRemoteContext(I input, LC ctx);

    protected abstract ResultAndUpdates<Y> processData(X data);

    /** {@inheritDoc} */
    @Override protected ComputationsChain<LC, K, V, I, I> trainingLoopStep() {
        ComputationsChain<LC, K, V, I, O> chain = Chains.create(new MetaoptimizerDistributedStep<>(metaoptimizer, this));
        return chain.thenLocally(metaoptimizer::localProcessor);
    }

    /** {@inheritDoc} */
    @Override protected I locallyProcessInitData(D data, LC locCtx) {
        return metaoptimizer.locallyProcessInitData(data, locCtx);
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldContinue(I data, LC locCtx) {
        return metaoptimizer.shouldContinue(data, locCtx);
    }

    /** {@inheritDoc} */
    @Override protected IgniteBinaryOperator<D> reduceDistributedInitData() {
        return metaoptimizer.initialReducer();
    }
}
