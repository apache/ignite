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
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

class MetaoptimizerDistributedStep<L extends HasTrainingUUID, K, V, G, I extends Serializable, O extends Serializable, IR, X, Y, D extends Serializable> implements org.apache.ignite.ml.trainers.group.chain.RemoteStep<L,K,V,G,I,O> {
    private final Metaoptimizer<IR, L, X, Y, I, D, O> metaoptimizer;
    private final SimpleGroupTrainer<L, K, V, D, ?, I, ?, ?, G, O, IR, X, Y> trainer;

    public MetaoptimizerDistributedStep(Metaoptimizer<IR, L, X, Y, I, D, O> metaoptimizer,
        SimpleGroupTrainer<L, K, V, D, ?, I, ?, ?, G, O, IR, X, Y> trainer) {
        this.metaoptimizer = metaoptimizer;
        this.trainer = trainer;
    }

    @Override public G extractRemoteContext(I input, L locCtx) {
        return trainer.extractRemoteContext(input, locCtx);
    }

    @Override
    public ResultAndUpdates<O> distributedWorker(I input, L locCtx, EntryAndContext<K, V, G> entryAndContext) {
        X apply = trainer.extractDataToProcessInTrainingLoop(entryAndContext);
        metaoptimizer.distributedPreprocess(input, apply);
        ResultAndUpdates<Y> res = trainer.processData(apply);
        O postprocessRes = metaoptimizer.distributedPostprocess(res.result());

        return ResultAndUpdates.of(postprocessRes).setUpdates(res.updates());
    }

    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier(I input, L locCtx) {
        // TODO: watch this code carefully.
        return () -> trainer.keysToProcessInTrainingLoop(locCtx);
    }

    @Override public O identity() {
        return metaoptimizer.postProcessIdentity();
    }

    @Override public O reduce(O arg1, O arg2) {
        return metaoptimizer.postProcessReducer(arg1, arg2);
    }
}
