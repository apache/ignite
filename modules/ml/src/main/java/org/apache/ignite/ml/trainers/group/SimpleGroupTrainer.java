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
import org.apache.ignite.ml.trainers.group.chain.DC;
import org.apache.ignite.ml.trainers.group.chain.DistributedTrainerWorkersChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasCacheContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

public abstract class SimpleGroupTrainer<LC extends HasTrainingUUID, K, V, D extends Serializable,
R extends Serializable, I extends Serializable,
M extends Model, T extends Distributive<K>,
G, O extends Serializable, IR, X, Y> extends
    GroupTrainer<LC, K, V, D, R, I, M, T, G> {
    private Metaoptimizer<IR, LC, X, Y, I, D, O> metaoptimizer;

    public SimpleGroupTrainer(
        IgniteCache<GroupTrainerCacheKey<K>, V> cache,
        Ignite ignite) {
        super(cache, ignite);
    }

    protected abstract X extractDataToProcessInTrainingLoop(EntryAndContext<K, V, G> entryAndContext);

    protected abstract Stream<GroupTrainerCacheKey<K>> keysToProcessInTrainingLoop(LC locCtx);

    protected abstract G extractRemoteContext(I input, LC ctx);

    protected abstract ResultAndUpdates<Y> processData(X data);

    @Override protected DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, I> trainingLoopStep() {
        DistributedTrainerWorkersChain<LC, K, V, I, GroupTrainingContext<K, V, LC>, O> chain = DC.create(new MetaoptimizerDistributedStep<>(metaoptimizer, this));
        return chain.thenLocally(metaoptimizer::localProcessor);
    }

    @Override protected I locallyProcessInitData(D data, LC locCtx) {
        return metaoptimizer.locallyProcessInitData(data, locCtx);
    }

    @Override protected boolean shouldContinue(I data, LC locCtx) {
        return metaoptimizer.shouldContinue(data, locCtx);
    }

    @Override protected D reduceGlobalInitData(D data1, D data2) {
        return metaoptimizer.initialReducer(data1, data2);
    }
}
