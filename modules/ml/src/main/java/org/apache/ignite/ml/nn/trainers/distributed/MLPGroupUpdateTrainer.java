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

package org.apache.ignite.ml.nn.trainers.distributed;

import java.io.Serializable;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.Metaoptimizer;
import org.apache.ignite.ml.trainers.group.MetaoptimizerGroupTrainer;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

public class MLPGroupUpdateTrainer extends
    MetaoptimizerGroupTrainer<MLPGroupUpdateTrainerLocalContext,
        Void,
        MLPGroupTrainingCacheValue,
        Serializable,
        MultilayerPerceptron,
        MLPGroupUpdateTrainingLoopData,
        MultilayerPerceptron,
        MLPGroupUpdateTrainerInput,
        MLPGroupUpdateTrainerContext,
        MLPGroupUpdateTrainingLoopData,
        MLPGroupUpdateTrainingLoopData,
        MLPGroupUpdateTrainingLoopData> {

    public MLPGroupUpdateTrainer(
        Metaoptimizer<MLPGroupUpdateTrainerLocalContext, MLPGroupUpdateTrainingLoopData, MLPGroupUpdateTrainingLoopData, MLPGroupUpdateTrainingLoopData, Serializable, MLPGroupUpdateTrainingLoopData> metaoptimizer,
        IgniteCache<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> cache,
        Ignite ignite) {
        super(metaoptimizer, cache, ignite);
    }

    @Override
    protected IgniteFunction<GroupTrainerCacheKey<Void>, ResultAndUpdates<Serializable>> distributedInitializer(
        MLPGroupUpdateTrainerInput data) {
        MultilayerPerceptron initPerceptron = data.mdl();

        return key -> {
            ResultAndUpdates<Serializable> res = ResultAndUpdates.empty();
            // For each key spawn neural network and put it into the cache.
            res.update(MLPCache.getOrCreate(Ignition.localIgnite()), key, new MLPGroupTrainingCacheValue(initPerceptron));

            return res;
        };
    }

    @Override
    protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainerContext>, MLPGroupUpdateTrainingLoopData> trainingLoopStepDataExtractor() {
        return null;
    }

    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> keysToProcessInTrainingLoop(
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    @Override
    protected IgniteSupplier<MLPGroupUpdateTrainerContext> remoteContextExtractor(MLPGroupUpdateTrainingLoopData input,
        MLPGroupUpdateTrainerLocalContext ctx) {
        return null;
    }

    @Override
    protected IgniteFunction<MLPGroupUpdateTrainingLoopData, ResultAndUpdates<MLPGroupUpdateTrainingLoopData>> dataProcessor() {
        return null;
    }

    @Override protected MLPGroupUpdateTrainerLocalContext initialLocalContext(MLPGroupUpdateTrainerInput data,
        UUID trainingUUID) {
        return null;
    }

    @Override protected IgniteSupplier<MLPGroupUpdateTrainerContext> extractContextForFinalResultCreation(
        MLPGroupUpdateTrainingLoopData data, MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    @Override
    protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> finalResultKeys(MLPGroupUpdateTrainingLoopData data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    @Override
    protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainerContext>, ResultAndUpdates<MultilayerPerceptron>> finalResultsExtractor() {
        return null;
    }

    @Override protected MultilayerPerceptron defaultFinalResult() {
        return null;
    }

    @Override protected IgniteBinaryOperator<MultilayerPerceptron> finalResultsReducer() {
        return null;
    }

    @Override
    protected MultilayerPerceptron mapFinalResult(MultilayerPerceptron res, MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    @Override protected void cleanup(MLPGroupUpdateTrainerLocalContext locCtx) {

    }
}
