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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.MetaoptimizerGroupTrainer;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.util.Utils;

public class MLPGroupUpdateTrainer<P> extends
    MetaoptimizerGroupTrainer<MLPGroupUpdateTrainerLocalContext,
        Void,
        MLPGroupTrainingCacheValue,
        Serializable,
        MultilayerPerceptron,
        MLPGroupUpdateTrainingLoopData,
        MultilayerPerceptron,
        MLPGroupUpdateTrainerInput,
        MLPGroupUpdateTrainerContext<P>,
        MLPGroupUpdateTrainingLoopData,
        MLPGroupUpdateTrainingLoopData<P>,
        P> {

    public MLPGroupUpdateTrainer(
        MLPMetaoptimizer<P> metaoptimizer,
        IgniteCache<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> cache,
        Ignite ignite, double tolerance) {
        super(metaoptimizer, cache, ignite);
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<GroupTrainerCacheKey<Void>, ResultAndUpdates<Serializable>> distributedInitializer(
        MLPGroupUpdateTrainerInput data) {
        MultilayerPerceptron initPerceptron = data.mdl();

        // For each key put initial network into the cache.
        return key -> {
            ResultAndUpdates<Serializable> res = ResultAndUpdates.empty();
            res.updateCache(MLPCache.getOrCreate(Ignition.localIgnite()), key, new MLPGroupTrainingCacheValue(initPerceptron));

            return res;
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainerContext<P>>, MLPGroupUpdateTrainingLoopData<P>> trainingLoopStepDataExtractor() {
        return entryAndContext -> {
            MLPGroupUpdateTrainerContext<P> ctx = entryAndContext.context();
            Map.Entry<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> entry = entryAndContext.entry();

            return new MLPGroupUpdateTrainingLoopData<P>(entry.getValue().perceptron(),
                ctx.updateCalculator(), ctx.stepsCnt(), ctx.updateReducer(), ctx.previousUpdate(), entry.getKey(), ctx.batchSupplier(), ctx.loss(), ctx.tolerance());
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> keysToProcessInTrainingLoop(
        MLPGroupUpdateTrainerLocalContext locCtx) {
        int trainingsCnt = locCtx.parallelTrainingsCnt();
        UUID uuid = locCtx.trainingUUID();

        return () -> IntStream.range(0, trainingsCnt).mapToObj(i -> new GroupTrainerCacheKey<Void>(i, null, uuid));
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<MLPGroupUpdateTrainerContext<P>> remoteContextExtractor(MLPGroupUpdateTrainingLoopData input,
        MLPGroupUpdateTrainerLocalContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<MLPGroupUpdateTrainingLoopData<P>, ResultAndUpdates<P>> dataProcessor() {
        return data -> {
            MultilayerPerceptron mlp = data.mlp();

            MultilayerPerceptron mlpCp = Utils.copy(mlp);
            ParameterUpdateCalculator<MultilayerPerceptron, P> updateCalculator = data.updateCalculator();
            IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss = data.loss();

            P curUpdate = data.previousUpdate();

            int steps = data.stepsCnt();
            List<P> updates = new ArrayList<>(steps);

            IgniteBiTuple<Matrix, Matrix> batch = data.batchSupplier().get();

            for (int i = 0; i < steps; i++) {
                Matrix input = batch.get1();
                Matrix truth = batch.get2();

                int batchSize = truth.columnSize();

                Matrix predicted = mlpCp.apply(truth);

                double err = MatrixUtil.zipFoldByColumns(predicted, truth, (predCol, truthCol) ->
                    loss.apply(truthCol).apply(predCol)).sum() / batchSize;

                if (err < data.tolerance())
                    break;

                updates.add(curUpdate);

                P update = updateCalculator.calculateNewUpdate(mlpCp, curUpdate, i, input, truth);
                mlpCp = updateCalculator.update(mlpCp, update);
            }

            P update = data.getUpdateReducer().apply(updates);

            MultilayerPerceptron newMlp = updateCalculator.update(mlp, data.previousUpdate());

            return new ResultAndUpdates<>(update).
                updateCache(MLPCache.getOrCreate(Ignition.localIgnite()), data.key(), new MLPGroupTrainingCacheValue(newMlp));
        };
    }

    /** {@inheritDoc} */
    @Override protected MLPGroupUpdateTrainerLocalContext initialLocalContext(MLPGroupUpdateTrainerInput data,
        UUID trainingUUID) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<MLPGroupUpdateTrainerContext<P>> extractContextForFinalResultCreation(
        MLPGroupUpdateTrainingLoopData data, MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> finalResultKeys(MLPGroupUpdateTrainingLoopData data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainerContext<P>>, ResultAndUpdates<MultilayerPerceptron>> finalResultsExtractor() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected MultilayerPerceptron defaultFinalResult() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteBinaryOperator<MultilayerPerceptron> finalResultsReducer() {
        // Just take any of MLPs since they will be in the same state.
        return (mlp1, mlp2) -> mlp1;
    }

    /** {@inheritDoc} */
    @Override protected MultilayerPerceptron mapFinalResult(MultilayerPerceptron res, MLPGroupUpdateTrainerLocalContext locCtx) {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(MLPGroupUpdateTrainerLocalContext locCtx) {

    }
}
